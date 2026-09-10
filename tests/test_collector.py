"""Collector tests. DESIGN.md section 8.

Three properties the whole pipeline depends on and that dbt cannot check,
because they are about the collector's behaviour rather than about the data it
has already written:

  1. Idempotency. Re-running a day upserts rather than duplicates. Every
     downstream model assumes one observation per repo per day; if that breaks,
     the SCD2 spine sees phantom state changes and the digest fills with
     events that never happened.

  2. Rate-limit handling. A secondary limit must be retried with backoff, while
     an exhausted primary budget must stop the run immediately rather than
     burning the remaining attempts.

  3. Config loading. A duplicate repo in repos.yml must not produce two rows
     competing for the same primary key.

The idempotency test runs against the LOCAL dev Postgres in Docker, never
against Neon. It writes to a scratch table it creates and drops itself, so it
cannot touch collected history even if pointed at the wrong database by
mistake.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, date
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "collector"))

import collect  # noqa: E402


LOCAL_DSN = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://radar:radar@localhost:{}/warehouse".format(
        os.environ.get("RADAR_PG_PORT", "5433").strip('"')
    ),
)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _repo_payload(**overrides):
    payload = {
        "id": 12345,
        "full_name": "owner/repo",
        "stargazers_count": 100,
        "forks_count": 10,
        "open_issues_count": 5,
        "subscribers_count": 3,
        "license": {"spdx_id": "MIT"},
        "archived": False,
        "disabled": False,
        "fork": False,
        "default_branch": "main",
        "description": "a repo",
        "homepage": None,
        "topics": ["ai", "agents"],
        "language": "Python",
        "size": 1000,
        "created_at": "2024-01-01T00:00:00Z",
        "pushed_at": "2026-09-01T00:00:00Z",
        "updated_at": "2026-09-01T00:00:00Z",
    }
    payload.update(overrides)
    return payload


def _response(status, headers=None, json_body=None):
    resp = Mock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.json.return_value = json_body or {}
    resp.raise_for_status = Mock()
    return resp


# --------------------------------------------------------------------------
# 1. idempotency
# --------------------------------------------------------------------------

@pytest.fixture
def scratch_table():
    """A throwaway copy of raw.repo_observations in the LOCAL dev warehouse.

    Created and dropped per test so a failure cannot leave residue, and so the
    test never writes into the real observation log.
    """
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(LOCAL_DSN, connect_timeout=5)
    except Exception as exc:
        pytest.skip(f"local dev Postgres not reachable ({exc}); run `make up`")

    schema = "collector_test"
    with conn.cursor() as cur:
        cur.execute(f"drop schema if exists {schema} cascade")
        cur.execute(f"create schema {schema}")
        # Structure copied from the real table, including the unique constraint
        # that makes the upsert work. Copying rather than redefining means the
        # test cannot drift from the real schema.
        cur.execute(
            f"create table {schema}.repo_observations "
            f"(like raw.repo_observations including all)"
        )
    conn.commit()

    yield conn, schema

    with conn.cursor() as cur:
        cur.execute(f"drop schema if exists {schema} cascade")
    conn.commit()
    conn.close()


def test_rerunning_the_same_day_upserts_rather_than_duplicating(scratch_table):
    """The property everything downstream assumes.

    Two runs on the same day must leave one row, with the SECOND run's values
    winning - a re-run exists to correct a bad collection, so it must overwrite.
    """
    conn, schema = scratch_table
    observed_at = datetime(2026, 9, 10, 6, 15, tzinfo=timezone.utc)
    tracked = collect.TrackedRepo("owner/repo", "mcp-servers", "high")

    sql = collect.UPSERT_SQL.replace(
        "raw.repo_observations", f"{schema}.repo_observations"
    )

    first = collect.build_observation(
        tracked, _repo_payload(stargazers_count=100), None, observed_at
    )
    second = collect.build_observation(
        tracked, _repo_payload(stargazers_count=175), None, observed_at
    )

    with conn.cursor() as cur:
        cur.execute(sql, first)
        cur.execute(sql, second)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"select count(*), max(stars) from {schema}.repo_observations")
        count, stars = cur.fetchone()

    assert count == 1, "re-running the same day must not create a second row"
    assert stars == 175, "the later run must win, so a bad collection can be corrected"


def test_different_days_are_separate_rows(scratch_table):
    """The flip side: the upsert must not collapse genuinely different days."""
    conn, schema = scratch_table
    tracked = collect.TrackedRepo("owner/repo", "mcp-servers", "high")
    sql = collect.UPSERT_SQL.replace(
        "raw.repo_observations", f"{schema}.repo_observations"
    )

    with conn.cursor() as cur:
        for day, stars in ((9, 100), (10, 120), (11, 130)):
            cur.execute(sql, collect.build_observation(
                tracked,
                _repo_payload(stargazers_count=stars),
                None,
                datetime(2026, 9, day, 6, 15, tzinfo=timezone.utc),
            ))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            f"select count(*), min(observed_date), max(observed_date) "
            f"from {schema}.repo_observations"
        )
        count, first_day, last_day = cur.fetchone()

    assert count == 3
    assert first_day == date(2026, 9, 9)
    assert last_day == date(2026, 9, 11)


# --------------------------------------------------------------------------
# 2. rate limiting
# --------------------------------------------------------------------------

def test_secondary_rate_limit_is_retried_with_backoff():
    """A 403 with budget REMAINING is a secondary limit: back off and retry.

    This is the case that is easy to get wrong, because it looks identical to
    an exhausted budget apart from the remaining count.
    """
    client = collect.GitHubClient("fake-token")

    responses = [
        _response(403, {"X-RateLimit-Remaining": "4000", "Retry-After": "1"}),
        _response(200, {"X-RateLimit-Remaining": "3999"}, {"full_name": "o/r"}),
    ]

    with patch.object(client.session, "get", side_effect=responses), \
         patch.object(collect.time, "sleep") as sleep:
        result = client.get("/repos/o/r")

    assert result == {"full_name": "o/r"}
    sleep.assert_called_once_with(1), "Retry-After must be honoured verbatim"


def test_secondary_limit_without_retry_after_backs_off_exponentially():
    """No Retry-After header means fall back to exponential backoff."""
    client = collect.GitHubClient("fake-token")

    responses = [
        _response(403, {"X-RateLimit-Remaining": "4000"}),
        _response(403, {"X-RateLimit-Remaining": "4000"}),
        _response(200, {"X-RateLimit-Remaining": "3999"}, {"ok": True}),
    ]

    with patch.object(client.session, "get", side_effect=responses), \
         patch.object(collect.time, "sleep") as sleep:
        client.get("/repos/o/r")

    waits = [call.args[0] for call in sleep.call_args_list]
    assert waits == [1, 2], "backoff must grow, and stay capped at 60s"


def test_exhausted_primary_budget_stops_the_run_immediately():
    """A 403 with ZERO remaining must raise, not retry.

    Retrying an exhausted primary budget wastes the run's remaining time on
    calls that cannot succeed until the hourly reset. Partial success is
    success - stop cleanly and let tomorrow fill the gap.
    """
    client = collect.GitHubClient("fake-token")

    resp = _response(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1757500000"})

    with patch.object(client.session, "get", return_value=resp) as get, \
         patch.object(collect.time, "sleep") as sleep:
        with pytest.raises(collect.RateLimitExhausted):
            client.get("/repos/o/r")

    assert get.call_count == 1, "must not retry an exhausted budget"
    sleep.assert_not_called()


def test_unauthenticated_token_is_rejected_loudly():
    """A limit of 60 means the token was not applied.

    Collecting at the unauthenticated rate would silently produce a partial,
    rate-limited history that looks fine until you query it weeks later.
    """
    client = collect.GitHubClient("fake-token")
    body = {"resources": {"core": {"remaining": 59, "limit": 60}}}

    with patch.object(client, "get", return_value=body):
        with pytest.raises(SystemExit, match="token is not being applied"):
            client.check_budget()


def test_missing_repo_returns_none_rather_than_raising():
    """A 404 is normal: repos get deleted, and plenty have never released."""
    client = collect.GitHubClient("fake-token")

    with patch.object(client.session, "get",
                      return_value=_response(404, {"X-RateLimit-Remaining": "4000"})):
        assert client.get("/repos/o/gone", allow_404=True) is None


# --------------------------------------------------------------------------
# 3. config
# --------------------------------------------------------------------------

def test_duplicate_repo_in_config_is_dropped(tmp_path):
    """Two entries for one repo would collide on the (repo, day) primary key."""
    config = tmp_path / "repos.yml"
    config.write_text(
        "repos:\n"
        "  - repo: owner/one\n"
        "    category: mcp-servers\n"
        "    priority: high\n"
        "  - repo: owner/one\n"
        "    category: coding-agents\n"
        "    priority: normal\n"
        "  - repo: owner/two\n"
        "    category: orchestration\n"
        "    priority: normal\n"
    )

    repos = collect.load_repos(str(config))

    assert [r.full_name for r in repos] == ["owner/one", "owner/two"]
    assert repos[0].category == "mcp-servers", "the first entry wins"


def test_missing_category_and_priority_get_defaults(tmp_path):
    config = tmp_path / "repos.yml"
    config.write_text("repos:\n  - repo: owner/bare\n")

    repo = collect.load_repos(str(config))[0]

    assert repo.category == "unclassified"
    assert repo.priority == "normal"


def test_empty_config_is_a_hard_failure(tmp_path):
    """Silently collecting nothing would look like a successful empty run."""
    config = tmp_path / "repos.yml"
    config.write_text("repos: []\n")

    with pytest.raises(SystemExit, match="No repos configured"):
        collect.load_repos(str(config))


# --------------------------------------------------------------------------
# 4. observation shape
# --------------------------------------------------------------------------

def test_release_body_hash_is_stable_and_absent_when_there_is_no_body():
    """The hash lets a re-published release be detected without diffing bodies."""
    tracked = collect.TrackedRepo("owner/repo", "mcp-servers", "high")
    at = datetime(2026, 9, 10, tzinfo=timezone.utc)

    with_body = collect.build_observation(
        tracked, _repo_payload(), {"tag_name": "v1", "body": "hello"}, at
    )
    same_body = collect.build_observation(
        tracked, _repo_payload(), {"tag_name": "v1", "body": "hello"}, at
    )
    no_body = collect.build_observation(
        tracked, _repo_payload(), {"tag_name": "v1", "body": None}, at
    )

    assert with_body["latest_release_body_sha256"] == same_body["latest_release_body_sha256"]
    assert no_body["latest_release_body_sha256"] is None


def test_repo_with_no_release_has_null_release_fields():
    """Plenty of tracked repos have never cut a release. That is not an error."""
    tracked = collect.TrackedRepo("owner/repo", "mcp-servers", "high")
    obs = collect.build_observation(
        tracked, _repo_payload(), None, datetime(2026, 9, 10, tzinfo=timezone.utc)
    )

    assert obs["latest_release_tag"] is None
    assert obs["raw_release_payload"] is None


def test_observed_date_is_derived_from_observed_at():
    """The two must never disagree, or the upsert key stops matching the data."""
    tracked = collect.TrackedRepo("owner/repo", "mcp-servers", "high")
    at = datetime(2026, 9, 10, 23, 59, 59, tzinfo=timezone.utc)

    obs = collect.build_observation(tracked, _repo_payload(), None, at)

    assert obs["observed_date"] == at.date()

#!/usr/bin/env python3
"""
oss-radar :: day-one collector

Fetches repository metadata and latest-release info for a configured set of
repos and appends one observation per repo per day to Postgres (Neon).

This is deliberately crude. It exists to START ACCUMULATING HISTORY TODAY,
because dbt snapshots can only capture change from the moment they first run
and GitHub will not tell you what a repo's license was three weeks ago.

Airflow, dbt, and change detection get built later, on top of this data.

Usage:
    export DATABASE_URL="postgresql://...@...neon.tech/neondb?sslmode=require"
    export GH_TOKEN="github_pat_..."
    python collect.py
    python collect.py --dry-run          # fetch, print, write nothing
    python collect.py --config repos.yml
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone

import psycopg
from psycopg.types.json import Json
import requests
import yaml
from dotenv import load_dotenv

# Loads .env into the environment if present. Safe to call in CI too: Actions
# doesn't create a .env file, so this is a silent no-op there and GH_TOKEN /
# DATABASE_URL come from the workflow's env: block (repo secrets) instead.
load_dotenv()

COLLECTOR_VERSION = "0.1.0"
API_ROOT = "https://api.github.com"
USER_AGENT = "oss-radar-collector"

# Stop cleanly rather than fighting the limit. Leaving headroom also avoids
# tripping GitHub's *secondary* limits, which are enforced separately from the
# primary hourly budget.
RATE_LIMIT_FLOOR = 100
REQUEST_TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("collect")


class RateLimitExhausted(Exception):
    """Raised when the remaining API budget drops below the floor."""


@dataclass
class TrackedRepo:
    full_name: str
    category: str
    priority: str
    notes: str | None = None


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------

def load_repos(path: str) -> list[TrackedRepo]:
    with open(path) as fh:
        raw = yaml.safe_load(fh)

    entries = raw.get("repos") or []
    if not entries:
        raise SystemExit(f"No repos configured in {path}")

    repos: list[TrackedRepo] = []
    seen: set[str] = set()
    for entry in entries:
        name = entry["repo"].strip()
        if name in seen:
            log.warning("Duplicate repo in config, skipping: %s", name)
            continue
        seen.add(name)
        repos.append(
            TrackedRepo(
                full_name=name,
                category=entry.get("category", "unclassified"),
                priority=entry.get("priority", "normal"),
                notes=entry.get("notes"),
            )
        )
    return repos


# --------------------------------------------------------------------------
# github client
# --------------------------------------------------------------------------

class GitHubClient:
    """Minimal client with rate-limit awareness and bounded retries.

    Deliberately not a general-purpose wrapper. It does the three things this
    collector needs and nothing else.
    """

    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                # GitHub requires a User-Agent on every API request.
                "User-Agent": USER_AGENT,
            }
        )
        self.remaining: int | None = None

    def _sleep_for_secondary_limit(self, resp: requests.Response, attempt: int) -> None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            wait = int(retry_after)
        else:
            wait = min(60, 2**attempt)
        log.warning("Secondary rate limit hit, sleeping %ss", wait)
        time.sleep(wait)

    def get(self, path: str, allow_404: bool = False) -> dict | None:
        url = f"{API_ROOT}{path}"

        for attempt in range(4):
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)

            remaining = resp.headers.get("X-RateLimit-Remaining")
            if remaining is not None:
                self.remaining = int(remaining)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404 and allow_404:
                return None

            # Primary budget exhausted — stop the whole run, do not retry.
            if resp.status_code == 403 and self.remaining == 0:
                reset = resp.headers.get("X-RateLimit-Reset", "unknown")
                raise RateLimitExhausted(f"Primary budget exhausted, resets at {reset}")

            # Secondary limit or transient throttle — back off and retry.
            if resp.status_code in (403, 429):
                self._sleep_for_secondary_limit(resp, attempt)
                continue

            if resp.status_code >= 500:
                wait = min(30, 2**attempt)
                log.warning("Server error %s on %s, retrying in %ss",
                            resp.status_code, path, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()

        raise RuntimeError(f"Giving up on {path} after 4 attempts")

    def check_budget(self) -> int:
        data = self.get("/rate_limit")
        remaining = data["resources"]["core"]["remaining"]
        limit = data["resources"]["core"]["limit"]
        self.remaining = remaining

        # A limit of 60 means the token was not applied — unauthenticated.
        if limit <= 60:
            raise SystemExit(
                f"Rate limit is {limit}, which means the token is not being "
                "applied. Check GH_TOKEN is set and valid."
            )

        log.info("Rate limit: %s/%s remaining", remaining, limit)
        return remaining

    def fetch_repo(self, full_name: str) -> dict | None:
        return self.get(f"/repos/{full_name}", allow_404=True)

    def fetch_latest_release(self, full_name: str) -> dict | None:
        # 404 is normal and expected: plenty of repos have never cut a release.
        return self.get(f"/repos/{full_name}/releases/latest", allow_404=True)


# --------------------------------------------------------------------------
# transform
# --------------------------------------------------------------------------

def _ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_observation(
    tracked: TrackedRepo,
    repo: dict,
    release: dict | None,
    observed_at: datetime,
) -> dict:
    body = (release or {}).get("body") or None
    body_hash = (
        hashlib.sha256(body.encode("utf-8")).hexdigest() if body else None
    )

    return {
        "repo_full_name": tracked.full_name,
        "category": tracked.category,
        "priority": tracked.priority,
        "observed_at": observed_at,
        "observed_date": observed_at.date(),
        "collector_version": COLLECTOR_VERSION,

        "stars": repo.get("stargazers_count"),
        "forks": repo.get("forks_count"),
        "open_issues": repo.get("open_issues_count"),
        "subscribers": repo.get("subscribers_count"),
        "license_spdx": (repo.get("license") or {}).get("spdx_id"),
        "is_archived": repo.get("archived"),
        "is_disabled": repo.get("disabled"),
        "is_fork": repo.get("fork"),
        "default_branch": repo.get("default_branch"),
        "description": repo.get("description"),
        "homepage": repo.get("homepage"),
        "topics": repo.get("topics") or [],
        "primary_language": repo.get("language"),
        "size_kb": repo.get("size"),
        "repo_created_at": _ts(repo.get("created_at")),
        "repo_pushed_at": _ts(repo.get("pushed_at")),
        "repo_updated_at": _ts(repo.get("updated_at")),

        "latest_release_tag": (release or {}).get("tag_name"),
        "latest_release_name": (release or {}).get("name"),
        "latest_release_published_at": _ts((release or {}).get("published_at")),
        "latest_release_is_prerelease": (release or {}).get("prerelease"),
        "latest_release_body": body,
        "latest_release_body_sha256": body_hash,

        "raw_repo_payload": Json(repo),
        "raw_release_payload": Json(release) if release else None,
    }


# --------------------------------------------------------------------------
# load
# --------------------------------------------------------------------------

COLUMNS = [
    "repo_full_name", "category", "priority",
    "observed_at", "observed_date", "collector_version",
    "stars", "forks", "open_issues", "subscribers", "license_spdx",
    "is_archived", "is_disabled", "is_fork", "default_branch",
    "description", "homepage", "topics", "primary_language", "size_kb",
    "repo_created_at", "repo_pushed_at", "repo_updated_at",
    "latest_release_tag", "latest_release_name", "latest_release_published_at",
    "latest_release_is_prerelease", "latest_release_body",
    "latest_release_body_sha256",
    "raw_repo_payload", "raw_release_payload",
]

# Upsert on (repo, day) so re-running on the same day corrects rather than
# duplicates. This is what makes the job safe to retry.
UPSERT_SQL = f"""
insert into raw.repo_observations ({", ".join(COLUMNS)})
values ({", ".join(f"%({c})s" for c in COLUMNS)})
on conflict (repo_full_name, observed_date) do update set
    {", ".join(
        f"{c} = excluded.{c}"
        for c in COLUMNS
        if c not in ("repo_full_name", "observed_date")
    )}
"""


def write_observations(conn, observations: list[dict]) -> None:
    # psycopg3's executemany pipelines statements when the server supports it
    # (Postgres 14+, which Neon is) -- no separate "batch" helper needed like
    # psycopg2's execute_batch.
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, observations)


def write_run_summary(conn, summary: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into raw.collection_runs (
                started_at, finished_at, collector_version,
                repos_attempted, repos_succeeded, repos_failed, repos_skipped,
                rate_limit_hit, rate_limit_remaining, notes
            ) values (
                %(started_at)s, %(finished_at)s, %(collector_version)s,
                %(attempted)s, %(succeeded)s, %(failed)s, %(skipped)s,
                %(rate_limit_hit)s, %(rate_limit_remaining)s, %(notes)s
            )
            """,
            summary,
        )


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="oss-radar day-one collector")
    parser.add_argument("--config", default="repos.yml")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch and report, write nothing")
    args = parser.parse_args()

    token = os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("GH_TOKEN is not set")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url and not args.dry_run:
        raise SystemExit("DATABASE_URL is not set")

    started_at = datetime.now(timezone.utc)
    tracked = load_repos(args.config)
    log.info("Collecting %d repos", len(tracked))

    client = GitHubClient(token)
    client.check_budget()

    observations: list[dict] = []
    failed: list[str] = []
    skipped: list[str] = []
    rate_limit_hit = False

    for i, repo_cfg in enumerate(tracked, start=1):
        if client.remaining is not None and client.remaining < RATE_LIMIT_FLOOR:
            log.warning(
                "Budget below floor (%s), stopping cleanly. %d repos not collected.",
                client.remaining, len(tracked) - i + 1,
            )
            rate_limit_hit = True
            skipped = [r.full_name for r in tracked[i - 1:]]
            break

        try:
            repo = client.fetch_repo(repo_cfg.full_name)
            if repo is None:
                # 404: renamed, deleted, or made private. Worth knowing about.
                log.error("NOT FOUND (renamed/deleted/private?): %s",
                          repo_cfg.full_name)
                failed.append(repo_cfg.full_name)
                continue

            release = client.fetch_latest_release(repo_cfg.full_name)
            observations.append(
                build_observation(repo_cfg, repo, release, started_at)
            )

            log.info(
                "[%2d/%2d] %-45s  stars=%-7s release=%s",
                i, len(tracked), repo_cfg.full_name,
                repo.get("stargazers_count"),
                (release or {}).get("tag_name", "-"),
            )

        except RateLimitExhausted as exc:
            log.warning("%s — stopping cleanly.", exc)
            rate_limit_hit = True
            skipped = [r.full_name for r in tracked[i - 1:]]
            break
        except Exception as exc:  # noqa: BLE001 — one repo must not kill the run
            log.exception("Failed on %s: %s", repo_cfg.full_name, exc)
            failed.append(repo_cfg.full_name)

    finished_at = datetime.now(timezone.utc)

    if args.dry_run:
        log.info("DRY RUN — %d observations built, nothing written",
                 len(observations))
        for obs in observations[:3]:
            printable = {
                k: v for k, v in obs.items() if not k.startswith("raw_")
            }
            print(json.dumps(printable, indent=2, default=str))
        return 0

    conn = psycopg.connect(database_url, connect_timeout=30)
    try:
        if observations:
            write_observations(conn, observations)
        write_run_summary(conn, {
            "started_at": started_at,
            "finished_at": finished_at,
            "collector_version": COLLECTOR_VERSION,
            "attempted": len(tracked),
            "succeeded": len(observations),
            "failed": len(failed),
            "skipped": len(skipped),
            "rate_limit_hit": rate_limit_hit,
            "rate_limit_remaining": client.remaining,
            "notes": (
                f"failed={failed}; skipped={skipped}" if failed or skipped else None
            ),
        })
        conn.commit()
    finally:
        conn.close()

    log.info(
        "Done in %.1fs — %d written, %d failed, %d skipped, %s budget left",
        (finished_at - started_at).total_seconds(),
        len(observations), len(failed), len(skipped), client.remaining,
    )

    # Partial success is an acceptable outcome and must not fail the workflow.
    # Only a total failure is worth alerting on.
    if not observations:
        log.error("Zero observations written — failing the run")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
"""
Render agg_weekly_digest into digests/YYYY-WNN.md.

Deliberately a dumb loop. All ranking, filtering and ordering already happened
in the warehouse (agg_weekly_digest is pre-sorted by digest_rank), so this
script's only job is to turn rows into Markdown. If a change is missing from a
digest, the reason is the materiality threshold, never a renderer bug.

Output is deterministic for a given warehouse state: same rows in, byte-identical
file out. Re-rendering a finished week produces no git diff.

Runs standalone (`make digest`) and from the radar_digest_weekly DAG.

    python include/render_digest.py                       # every week present
    python include/render_digest.py --week 2026-W37       # one week
    python include/render_digest.py --dry-run             # print, do not write
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg


# ---------------------------------------------------------------------------
# presentation
# ---------------------------------------------------------------------------

MATERIALITY_ORDER = ["critical", "high", "medium", "low"]

MATERIALITY_LABEL = {
    "critical": "Critical",
    "high":     "High",
    "medium":   "Medium",
    "low":      "Low",
}

CHANGE_LABEL = {
    "archived":                  "archived",
    "unarchived":                "un-archived",
    "license_changed":           "licence changed",
    "renamed_or_transferred":    "renamed or transferred",
    "default_branch_changed":    "default branch changed",
    "description_changed":       "description changed",
    "homepage_changed":          "homepage changed",
    "topics_changed":            "topics changed",
    "tracking_category_changed": "tracking category changed",
    "tracking_priority_changed": "tracking priority changed",
    "breaking_release":          "breaking release",
    "major_release":             "major release",
    "minor_release":             "minor release",
    "patch_release":             "patch release",
    "release_unclassified":      "release (unclassified tag)",
    "went_stale":                "went stale",
    "star_spike":                "star spike",
}


@dataclass
class Entry:
    rank: int
    repo: str
    category: str
    priority: str
    change_type: str
    materiality: str
    detected_at: date
    before: str | None
    after: str | None
    evidence: dict
    event_count: int = 1


@dataclass
class Week:
    iso_year: int
    iso_week: int
    week_start: date
    entries: list[Entry]
    missing_observations: int
    repos_with_gaps: int
    dates_with_gaps: int

    @property
    def label(self) -> str:
        return f"{self.iso_year}-W{self.iso_week:02d}"

    @property
    def week_end(self) -> date:
        return self.week_start + timedelta(days=6)

    def is_partial(self, today: date) -> bool:
        return today <= self.week_end


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------

def fetch_weeks(conn, schema: str, only_week: str | None) -> list[Week]:
    sql = f"""
        select
            detected_iso_year, detected_iso_week, detected_week, digest_rank,
            repo_full_name, category, priority, change_type, materiality,
            detected_at, before_value, after_value, evidence, event_count,
            week_missing_observations, week_repos_with_gaps, week_dates_with_gaps
        from {schema}.agg_weekly_digest
        order by detected_week, digest_rank
    """
    rows = conn.execute(sql).fetchall()

    weeks: dict[str, Week] = {}
    for r in rows:
        (iso_year, iso_week, week_start, rank, repo, category, priority,
         change_type, materiality, detected_at, before, after, evidence,
         event_count, missing, repos_gaps, dates_gaps) = r
        label = f"{iso_year}-W{iso_week:02d}"
        if only_week and label != only_week:
            continue
        week = weeks.setdefault(label, Week(
            iso_year=iso_year, iso_week=iso_week, week_start=week_start,
            entries=[], missing_observations=missing,
            repos_with_gaps=repos_gaps, dates_with_gaps=dates_gaps,
        ))
        week.entries.append(Entry(
            rank=rank, repo=repo, category=category, priority=priority,
            change_type=change_type, materiality=materiality,
            detected_at=detected_at, before=before, after=after,
            evidence=json.loads(evidence) if evidence else {},
            event_count=event_count,
        ))
    return [weeks[k] for k in sorted(weeks)]


def fetch_coverage(conn, schema: str, week: Week) -> dict:
    """Context for the footer: how much was actually watched this week."""
    row = conn.execute(f"""
        select
            count(distinct github_repo_id)                       as repos,
            count(*)                                             as observations,
            count(distinct observed_date)                        as days
        from {schema.replace('_marts', '_intermediate')}.int_activity_signals
        where observed_date between %s and %s
    """, (week.week_start, week.week_end)).fetchone()
    return {"repos": row[0], "observations": row[1], "days": row[2]}


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------

def describe_change(e: Entry) -> str:
    label = CHANGE_LABEL.get(e.change_type, e.change_type.replace("_", " "))
    # A collapsed line summarises several events: "3 patch releases".
    if e.event_count > 1:
        label = f"{e.event_count} {label}s" if not label.endswith("s") else f"{e.event_count} {label}"
    if e.change_type in ("went_stale", "star_spike"):
        return label
    if e.before is not None and e.after is not None:
        return f"{label} `{e.before}` → `{e.after}`"
    if e.after is not None:
        return f"{label} → `{e.after}`"
    return label


def clean_quote(text: str) -> str:
    """Strip Markdown structure from a quoted release-note line so it reads as
    a sentence inside the blockquote rather than nesting a heading or bullet."""
    line = text.strip().splitlines()[0] if text.strip() else ""
    line = line.lstrip("#").strip()          # heading markers
    if line[:2] in ("- ", "* ", "+ "):        # list bullets
        line = line[2:].strip()
    return line


def evidence_lines(e: Entry) -> list[str]:
    """The verifiable part. What a reader needs to judge the entry themselves."""
    ev = e.evidence
    out: list[str] = []

    if e.change_type == "breaking_release":
        text = ev.get("matched_text") or ""
        if text.strip():
            # Quote the matched line from the release notes.
            out.append(f"> {clean_quote(text)}")
        markers = ev.get("breaking_markers")
        if markers:
            out.append(f"<sub>matched: `{markers}`</sub>")

    elif e.change_type in ("major_release", "minor_release", "patch_release",
                           "release_unclassified"):
        published = ev.get("published_at")
        if published:
            out.append(f"<sub>published {published[:10]}</sub>")

    elif e.change_type == "went_stale":
        days = ev.get("days_since_push")
        threshold = ev.get("threshold_days")
        if days is not None:
            out.append(f"<sub>{days} days since last push (threshold {threshold})</sub>")

    elif e.change_type == "star_spike":
        recent = ev.get("stars_per_day_recent")
        base = ev.get("stars_per_day_baseline")
        mult = ev.get("multiple")
        if recent is not None:
            out.append(f"<sub>{recent}/day recent vs {base}/day baseline ({mult}×)</sub>")

    else:
        # Metadata change: before/after is the whole story and is already in
        # the heading, so nothing further is needed.
        pass

    return out


def render_week(week: Week, coverage: dict, today: date) -> str:
    partial = week.is_partial(today)
    lines: list[str] = []

    title_suffix = " (in progress)" if partial else ""
    lines.append(f"# oss-radar · {week.label}{title_suffix}")
    lines.append("")
    span = f"{week.week_start.isoformat()} to {week.week_end.isoformat()}"
    if partial:
        lines.append(f"Week of {span}. **Partial** — rendered {today.isoformat()}, "
                     f"before the week ended. Re-rendered daily until complete.")
    else:
        lines.append(f"Week of {span}.")
    lines.append("")

    # Coverage caveat. The whole point of int_collection_gaps.
    if week.missing_observations > 0:
        lines.append(
            f"> **Coverage gap.** {week.missing_observations} observation(s) missing "
            f"across {week.repos_with_gaps} repo(s) on {week.dates_with_gaps} day(s). "
            f"Changes in those repos on those days may not appear here."
        )
        lines.append("")

    if not week.entries:
        lines.append("_No changes above the materiality threshold this week._")
        lines.append("")
    else:
        by_level: dict[str, list[Entry]] = {}
        for e in week.entries:
            by_level.setdefault(e.materiality, []).append(e)

        for level in MATERIALITY_ORDER:
            entries = by_level.get(level)
            if not entries:
                continue
            lines.append(f"## {MATERIALITY_LABEL[level]}")
            lines.append("")
            for e in entries:
                lines.append(f"**{e.repo}** — {describe_change(e)}  ")
                for ev_line in evidence_lines(e):
                    lines.append(ev_line + "  ")
                meta = f"<sub>{e.category} · {e.priority} priority · detected {e.detected_at.isoformat()}</sub>"
                lines.append(meta)
                lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        f"<sub>{coverage['repos']} repos watched · {coverage['observations']} observations "
        f"over {coverage['days']} day(s) · {len(week.entries)} change(s) above threshold · "
        f"ranked by materiality, adjusted for repo priority and staleness</sub>"
    )
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def build_dsn() -> str:
    """Host-side default reaches the compose-published port; inside Airflow the
    env overrides point at the service name."""
    explicit = os.environ.get("RADAR_WAREHOUSE_DSN")
    if explicit:
        return explicit
    host = os.environ.get("RADAR_PG_HOST", "localhost")
    port = os.environ.get("RADAR_PG_PORT", "5433").strip('"')
    return f"postgresql://radar:radar@{host}:{port}/warehouse"


def main() -> int:
    # Windows consoles default to cp1252, which cannot encode the arrows and
    # dashes in the digest. The FILE is always written as UTF-8 regardless;
    # this only affects --dry-run and log output.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--week", help="ISO week label, e.g. 2026-W37. Default: every week present.")
    ap.add_argument("--schema", default=os.environ.get("RADAR_MARTS_SCHEMA", "dbt_tapan_marts"),
                    help="Marts schema to read from (default: $RADAR_MARTS_SCHEMA or dbt_tapan_marts).")
    ap.add_argument("--out", default=os.environ.get("DIGESTS_DIR", "digests"),
                    help="Output directory (default: $DIGESTS_DIR or ./digests).")
    ap.add_argument("--dry-run", action="store_true", help="Print instead of writing.")
    ap.add_argument("--today", help="Override today's date (YYYY-MM-DD), for reproducible tests.")
    args = ap.parse_args()

    today = date.fromisoformat(args.today) if args.today else datetime.now(timezone.utc).date()
    out_dir = Path(args.out)

    with psycopg.connect(build_dsn(), connect_timeout=30) as conn:
        weeks = fetch_weeks(conn, args.schema, args.week)
        if not weeks:
            print("no digest rows found" + (f" for {args.week}" if args.week else ""), file=sys.stderr)
            return 0

        written: list[str] = []
        for week in weeks:
            coverage = fetch_coverage(conn, args.schema, week)
            body = render_week(week, coverage, today)
            target = out_dir / f"{week.label}.md"

            if args.dry_run:
                print(body)
                continue

            out_dir.mkdir(parents=True, exist_ok=True)
            previous = target.read_text(encoding="utf-8") if target.exists() else None
            if previous == body:
                print(f"unchanged  {target}")
                continue
            target.write_text(body, encoding="utf-8", newline="\n")
            written.append(str(target))
            print(f"{'updated  ' if previous else 'written  '}{target}  ({len(week.entries)} entries)")

        # Machine-readable summary for the DAG's downstream task.
        print(json.dumps({"written": written}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

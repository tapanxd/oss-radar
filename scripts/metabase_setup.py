"""Build the oss-radar dashboard in Metabase from code.

Run by `make dashboard`. Talks to the Metabase container's REST API and does,
in order:

  1. First-run setup (admin user) if Metabase has never been set up, otherwise
     log in as that admin.
  2. Connect the dev warehouse as a data source, filtered to the marts schema.
  3. Create or update every saved question in the `oss-radar` collection.
  4. Create or update the `oss-radar` dashboard and lay the questions out.

Every step finds existing objects by name and updates them in place, so the
script can be re-run after a change to a query below and the dashboard URL,
card ids and any manual filters people added stay put. Nothing here touches
the warehouse itself; Metabase only reads it.

Questions are native SQL against the marts, not Metabase's query builder, so
the SQL is reviewable here and identical to what a reader would run by hand.
The three DESIGN.md section 11 views - category pulse over time, change feed,
repo timeline - map to the three dashboard sections.

Standard library only: this runs from the host venv and needs no extra pins.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

METABASE_URL = os.environ.get("RADAR_METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@oss-radar.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "RadarAdmin2026!")

# The marts schema as Metabase sees it. dbt's dev target builds into
# <profile schema>_marts; the default matches profiles.yml. Point this at
# analytics_marts with the Neon details below to dashboard production instead.
MARTS_SCHEMA = os.environ.get("RADAR_MARTS_SCHEMA", "dbt_tapan_marts")
WAREHOUSE = {
    # Metabase runs inside the compose network, so it reaches Postgres by
    # service name, not by the host's localhost:5433.
    "host": os.environ.get("RADAR_MB_WAREHOUSE_HOST", "postgres"),
    "port": int(os.environ.get("RADAR_MB_WAREHOUSE_PORT", "5432")),
    "dbname": os.environ.get("RADAR_MB_WAREHOUSE_DB", "warehouse"),
    "user": os.environ.get("RADAR_MB_WAREHOUSE_USER", "radar"),
    "password": os.environ.get("RADAR_MB_WAREHOUSE_PASSWORD", "radar"),
    "ssl": os.environ.get("RADAR_MB_WAREHOUSE_SSL", "false").lower() == "true",
}

COLLECTION_NAME = "oss-radar"
DASHBOARD_NAME = "oss-radar"
DATABASE_NAME = "oss-radar warehouse"


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class Metabase:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.session:
            req.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:2000]
            raise SystemExit(f"{method} {path} -> HTTP {e.code}\n{detail}") from None
        return json.loads(raw) if raw else None

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body):
        return self.call("POST", path, body)

    def put(self, path, body):
        return self.call("PUT", path, body)


def wait_for_health(mb: Metabase, attempts: int = 90) -> None:
    print(f"waiting for metabase at {mb.base_url}", end="", flush=True)
    for _ in range(attempts):
        try:
            if mb.get("/api/health").get("status") == "ok":
                print(" ready")
                return
        except (urllib.error.URLError, SystemExit, ConnectionError, OSError):
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    raise SystemExit("\nmetabase did not become healthy; `docker logs radar-metabase`")


# ---------------------------------------------------------------------------
# Setup / login
# ---------------------------------------------------------------------------

def authenticate(mb: Metabase) -> None:
    props = mb.get("/api/session/properties")
    token = props.get("setup-token")
    if token:
        print(f"first run: creating admin {ADMIN_EMAIL}")
        resp = mb.post("/api/setup", {
            "token": token,
            "user": {
                "first_name": "Radar",
                "last_name": "Admin",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "site_name": "oss-radar",
            },
            "prefs": {"site_name": "oss-radar", "site_locale": "en", "allow_tracking": "false"},
        })
        mb.session = resp["id"]
    else:
        resp = mb.post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        mb.session = resp["id"]
        print(f"logged in as {ADMIN_EMAIL}")


# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------

def ensure_database(mb: Metabase) -> int:
    existing = mb.get("/api/database")
    rows = existing["data"] if isinstance(existing, dict) else existing
    details = {
        **WAREHOUSE,
        # Only the marts are for consumption; staging and intermediate stay
        # out of the browser so nobody builds a question on a view that the
        # next model change renames.
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": MARTS_SCHEMA,
        "tunnel-enabled": False,
        "advanced-options": False,
    }
    for db in rows:
        if db["name"] == DATABASE_NAME:
            mb.put(f"/api/database/{db['id']}", {"details": details})
            mb.post(f"/api/database/{db['id']}/sync_schema", {})
            print(f"warehouse connection updated (id {db['id']}, schema {MARTS_SCHEMA})")
            return db["id"]
    created = mb.post("/api/database", {
        "name": DATABASE_NAME,
        "engine": "postgres",
        "details": details,
        "is_full_sync": True,
        "is_on_demand": False,
    })
    print(f"warehouse connected (id {created['id']}, schema {MARTS_SCHEMA})")
    return created["id"]


def remove_sample_database(mb: Metabase) -> None:
    """The bundled Sample Database is noise next to a single real source."""
    existing = mb.get("/api/database")
    rows = existing["data"] if isinstance(existing, dict) else existing
    for db in rows:
        if db.get("is_sample"):
            mb.call("DELETE", f"/api/database/{db['id']}")
            print("sample database removed")


# ---------------------------------------------------------------------------
# Collection, cards, dashboard
# ---------------------------------------------------------------------------

def ensure_collection(mb: Metabase) -> int:
    for c in mb.get("/api/collection"):
        if c.get("name") == COLLECTION_NAME and not c.get("archived"):
            return c["id"]
    created = mb.post("/api/collection", {"name": COLLECTION_NAME, "parent_id": None})
    print(f"collection '{COLLECTION_NAME}' created")
    return created["id"]


def collection_items(mb: Metabase, collection_id: int, model: str) -> dict[str, int]:
    resp = mb.get(f"/api/collection/{collection_id}/items?models={model}")
    items = resp["data"] if isinstance(resp, dict) else resp
    return {i["name"]: i["id"] for i in items if i.get("model") == model}


def sql(query: str) -> str:
    return query.strip().replace("{schema}", MARTS_SCHEMA)


def card_spec(name, description, query, display, viz=None):
    return {
        "name": name,
        "description": description,
        "display": display,
        "query": sql(query),
        "viz": viz or {},
    }


# Twenty-four column grid. Scalars are 4x3; charts 12x6; tables full or 2/3.
CARDS = [
    # -- headline numbers --------------------------------------------------
    card_spec(
        "Repos watched", "Rows in dim_repos.",
        "select count(*) as repos from {schema}.dim_repos",
        "scalar"),
    card_spec(
        "Days of history", "Longest tracked span across repos. Windowed signals need 7.",
        "select max(days_tracked) as days from {schema}.dim_repos",
        "scalar"),
    card_spec(
        "Change events", "All detected changes, every materiality.",
        "select count(*) as events from {schema}.fct_change_events",
        "scalar"),
    card_spec(
        "Critical + high", "Events at critical or high materiality after priority and staleness adjustment.",
        "select count(*) as events from {schema}.fct_change_events where materiality in ('critical', 'high')",
        "scalar"),
    card_spec(
        "Stale repos", "No push in the staleness window, as of the latest observation.",
        "select count(*) as stale from {schema}.dim_repos where is_stale",
        "scalar"),
    card_spec(
        "Archived", "Archived on GitHub as of the latest observation.",
        "select count(*) as archived from {schema}.dim_repos where is_archived",
        "scalar"),

    # -- category pulse over time ----------------------------------------
    card_spec(
        "Material events per week by category",
        "agg_category_pulse.material_events, stacked by category. One bar per ISO week.",
        """
        select pulse_week, category, material_events
        from {schema}.agg_category_pulse
        order by pulse_week, category
        """,
        "bar",
        {"graph.dimensions": ["pulse_week", "category"],
         "graph.metrics": ["material_events"],
         "stackable.stack_type": "stacked",
         "graph.x_axis.title_text": "week",
         "graph.y_axis.title_text": "material events"}),
    card_spec(
        "Stars gained per week by category",
        "agg_category_pulse.stars_gained. Partial weeks undercount; see the digest caveat.",
        """
        select pulse_week, category, stars_gained
        from {schema}.agg_category_pulse
        order by pulse_week, category
        """,
        "line",
        {"graph.dimensions": ["pulse_week", "category"],
         "graph.metrics": ["stars_gained"],
         "graph.x_axis.title_text": "week",
         "graph.y_axis.title_text": "stars gained"}),
    card_spec(
        "Category pulse, latest week",
        "One row per category for the most recent week in agg_category_pulse.",
        """
        select
            category,
            repos_observed,
            stars_gained,
            releases,
            breaking_releases,
            repos_went_stale,
            repos_archived,
            repos_stale,
            round(avg_days_since_push, 1) as avg_days_since_push,
            material_events,
            missing_observations
        from {schema}.agg_category_pulse
        where pulse_week = (select max(pulse_week) from {schema}.agg_category_pulse)
        order by material_events desc, stars_gained desc
        """,
        "table"),
    card_spec(
        "Collection health",
        "Missing observation-days per week across all repos. Non-zero means the collector skipped something; the gap cannot be backfilled.",
        """
        select
            pulse_week,
            sum(missing_observations) as missing_observations,
            sum(repos_with_gaps)      as repos_with_gaps
        from {schema}.agg_category_pulse
        group by pulse_week
        order by pulse_week
        """,
        "line",
        {"graph.dimensions": ["pulse_week"],
         "graph.metrics": ["missing_observations", "repos_with_gaps"],
         "graph.x_axis.title_text": "week"}),

    # -- change feed -------------------------------------------------------
    card_spec(
        "Change feed",
        "fct_change_events, newest first, then by materiality score. Evidence is the JSON that justifies each row.",
        """
        select
            detected_at,
            repo_full_name,
            category,
            change_type,
            materiality,
            materiality_score,
            before_value,
            after_value,
            evidence
        from {schema}.fct_change_events
        order by detected_at desc, materiality_score desc, repo_full_name
        """,
        "table"),
    card_spec(
        "Change mix",
        "Events by change type, stacked by materiality.",
        """
        select change_type, materiality, count(*) as events
        from {schema}.fct_change_events
        group by change_type, materiality
        order by events desc
        """,
        "row",
        {"graph.dimensions": ["change_type", "materiality"],
         "graph.metrics": ["events"],
         "stackable.stack_type": "stacked"}),

    # -- repo timeline -----------------------------------------------------
    card_spec(
        "Stars gained this month",
        "Top 15 repos by stars gained in the latest month of agg_repo_timeline.",
        """
        select repo_full_name, stars_gained_in_month
        from {schema}.agg_repo_timeline
        where timeline_month = (select max(timeline_month) from {schema}.agg_repo_timeline)
        order by stars_gained_in_month desc, repo_full_name
        limit 15
        """,
        "row",
        {"graph.dimensions": ["repo_full_name"],
         "graph.metrics": ["stars_gained_in_month"]}),
    card_spec(
        "Repo timeline",
        "agg_repo_timeline: one row per repo per month. Sorted by month, then by what happened.",
        """
        select
            timeline_month,
            repo_full_name,
            category,
            priority,
            days_observed,
            missing_observation_days,
            stars_at_month_end,
            stars_gained_in_month,
            releases,
            change_events,
            material_events,
            is_stale_at_month_end,
            change_types
        from {schema}.agg_repo_timeline
        order by timeline_month desc, material_events desc, stars_gained_in_month desc, repo_full_name
        """,
        "table"),
]


def ensure_cards(mb: Metabase, collection_id: int, database_id: int) -> dict[str, int]:
    existing = collection_items(mb, collection_id, "card")
    ids: dict[str, int] = {}
    for spec in CARDS:
        body = {
            "name": spec["name"],
            "description": spec["description"],
            "display": spec["display"],
            "collection_id": collection_id,
            "dataset_query": {
                "type": "native",
                "native": {"query": spec["query"], "template-tags": {}},
                "database": database_id,
            },
            "visualization_settings": spec["viz"],
        }
        if spec["name"] in existing:
            card_id = existing[spec["name"]]
            mb.put(f"/api/card/{card_id}", body)
            action = "updated"
        else:
            card_id = mb.post("/api/card", body)["id"]
            action = "created"
        ids[spec["name"]] = card_id
        print(f"  {action:7s} card {card_id:>3}  {spec['name']}")
    return ids


def text_card(neg_id: int, text: str, row: int, col: int, size_x: int, size_y: int, heading=True):
    return {
        "id": neg_id,
        "card_id": None,
        "dashboard_tab_id": None,
        "row": row, "col": col, "size_x": size_x, "size_y": size_y,
        "series": [], "parameter_mappings": [],
        "visualization_settings": {
            "virtual_card": {
                "name": None,
                "display": "heading" if heading else "text",
                "visualization_settings": {},
                "dataset_query": {},
                "archived": False,
            },
            "text": text,
        },
    }


def dashcard(neg_id: int, card_id: int, row: int, col: int, size_x: int, size_y: int):
    return {
        "id": neg_id,
        "card_id": card_id,
        "dashboard_tab_id": None,
        "row": row, "col": col, "size_x": size_x, "size_y": size_y,
        "series": [], "parameter_mappings": [], "visualization_settings": {},
    }


def ensure_dashboard(mb: Metabase, collection_id: int, cards: dict[str, int]) -> int:
    existing = collection_items(mb, collection_id, "dashboard")
    if DASHBOARD_NAME in existing:
        dash_id = existing[DASHBOARD_NAME]
        action = "updated"
    else:
        dash_id = mb.post("/api/dashboard", {
            "name": DASHBOARD_NAME,
            "collection_id": collection_id,
            "description": "Material changes across the tracked repos. Category pulse, change feed, repo timeline.",
        })["id"]
        action = "created"

    c = cards
    n = iter(range(-1, -100, -1))  # negative ids mean "new dashcard" to the API
    layout = [
        # headline row
        dashcard(next(n), c["Repos watched"],   0, 0, 4, 3),
        dashcard(next(n), c["Days of history"], 0, 4, 4, 3),
        dashcard(next(n), c["Change events"],   0, 8, 4, 3),
        dashcard(next(n), c["Critical + high"], 0, 12, 4, 3),
        dashcard(next(n), c["Stale repos"],     0, 16, 4, 3),
        dashcard(next(n), c["Archived"],        0, 20, 4, 3),

        text_card(next(n), "Category pulse over time", 3, 0, 24, 1),
        dashcard(next(n), c["Material events per week by category"], 4, 0, 12, 6),
        dashcard(next(n), c["Stars gained per week by category"],    4, 12, 12, 6),
        dashcard(next(n), c["Category pulse, latest week"],          10, 0, 16, 6),
        dashcard(next(n), c["Collection health"],                    10, 16, 8, 6),

        text_card(next(n), "Change feed", 16, 0, 24, 1),
        dashcard(next(n), c["Change feed"], 17, 0, 17, 9),
        dashcard(next(n), c["Change mix"],  17, 17, 7, 9),

        text_card(next(n), "Repo timeline", 26, 0, 24, 1),
        dashcard(next(n), c["Stars gained this month"], 27, 0, 8, 9),
        dashcard(next(n), c["Repo timeline"],           27, 8, 16, 9),
    ]
    mb.put(f"/api/dashboard/{dash_id}", {"dashcards": layout})
    print(f"dashboard {action} (id {dash_id}, {len(layout)} cards)")
    return dash_id


# ---------------------------------------------------------------------------

def main() -> None:
    mb = Metabase(METABASE_URL)
    wait_for_health(mb)
    authenticate(mb)
    remove_sample_database(mb)
    database_id = ensure_database(mb)
    collection_id = ensure_collection(mb)
    cards = ensure_cards(mb, collection_id, database_id)
    dash_id = ensure_dashboard(mb, collection_id, cards)
    print(f"\n{METABASE_URL}/dashboard/{dash_id}  (login {ADMIN_EMAIL})")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

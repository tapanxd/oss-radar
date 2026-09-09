-- oss-radar :: day-one collector schema
-- Run once against Neon before the first collection.
--   psql "$DATABASE_URL" -f schema.sql

create schema if not exists raw;

-- Append-only observation log.
--
-- Design note: this is deliberately NOT a "current state" table. dbt snapshots
-- do not exist yet, and a snapshot can only capture changes from the moment it
-- first runs. By logging one full observation per repo per day from day one,
-- the entire history is preserved and the snapshot layer can be built later
-- against real accumulated data rather than starting from empty.
--
-- Volume: ~40 repos x 365 days x ~2KB = roughly 30 MB/year. Not a concern.

create table if not exists raw.repo_observations (
    id                            bigserial primary key,

    -- identity + config
    repo_full_name                text        not null,
    category                      text,
    priority                      text,

    -- collection metadata
    observed_at                   timestamptz not null default now(),
    observed_date                 date        not null,
    collector_version             text        not null,

    -- repo metadata (the fields change detection will diff)
    stars                         integer,
    forks                         integer,
    open_issues                   integer,
    subscribers                   integer,
    license_spdx                  text,
    is_archived                   boolean,
    is_disabled                   boolean,
    is_fork                       boolean,
    default_branch                text,
    description                   text,
    homepage                      text,
    topics                        text[],
    primary_language              text,
    size_kb                       integer,
    repo_created_at               timestamptz,
    repo_pushed_at                timestamptz,
    repo_updated_at               timestamptz,

    -- latest release (null if the repo has never released)
    latest_release_tag            text,
    latest_release_name           text,
    latest_release_published_at   timestamptz,
    latest_release_is_prerelease  boolean,
    latest_release_body           text,
    latest_release_body_sha256    text,

    -- full API payloads, kept so a field not extracted today can still be
    -- recovered from history later without re-collecting
    raw_repo_payload              jsonb,
    raw_release_payload           jsonb,

    -- one observation per repo per day; a re-run overwrites rather than duplicates
    unique (repo_full_name, observed_date)
);

create index if not exists idx_repo_observations_repo_date
    on raw.repo_observations (repo_full_name, observed_date desc);

create index if not exists idx_repo_observations_date
    on raw.repo_observations (observed_date desc);

-- Per-run summary, so a gap in collection is itself a queryable fact rather
-- than something you discover months later by noticing missing days.
create table if not exists raw.collection_runs (
    run_id             bigserial primary key,
    started_at         timestamptz not null,
    finished_at        timestamptz,
    collector_version  text        not null,
    repos_attempted    integer     not null default 0,
    repos_succeeded    integer     not null default 0,
    repos_failed       integer     not null default 0,
    repos_skipped      integer     not null default 0,
    rate_limit_hit     boolean     not null default false,
    rate_limit_remaining integer,
    notes              text
);

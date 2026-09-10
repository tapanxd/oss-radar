-- Grain: one row per repo per date on which the repo should have been
-- observed but was not.
--
-- This model exists so the digest can state its blind spots. Every other model
-- describes what WAS seen; without this one, a week where the collector was
-- silently disabled looks identical to a week where nothing happened, and the
-- digest quietly implies continuous coverage it does not have.
--
-- DESIGN.md section 10 names two real ways this happens:
--   - GitHub disables scheduled workflows on repos inactive for ~60 days, and
--     the cron then dies without any notification.
--   - Neon is unreachable, or the collector hit its rate-limit floor and
--     stopped cleanly partway through the repo list.
--
-- Those two cases are DIFFERENT and are distinguished here. "The collector did
-- not run" is an infrastructure failure affecting every repo; "the collector
-- ran but skipped this repo" is a rate-limit or per-repo fetch failure. A
-- reader needs to know which, because they imply different amounts of doubt
-- about the rest of the digest.
--
-- Expected coverage starts at each repo's FIRST observation, not at the
-- project-wide start date: a repo added to repos.yml last week has no gap for
-- the months before it was tracked.

with observations as (

    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date
    from {{ ref('stg_repo_observations') }}

),

collection_runs as (

    select
        run_date,
        is_partial,
        is_incomplete,
        repos_succeeded,
        repos_failed,
        repos_skipped,
        rate_limit_hit
    from {{ ref('stg_collection_runs') }}

),

observation_bounds as (

    select
        min(observed_date) as first_observed_date,
        max(observed_date) as last_observed_date
    from observations

),

-- Every calendar date the pipeline has been alive for.
--
-- generate_series rather than dbt_utils.date_spine: date_spine emits its own
-- nested WITH clause, which cannot see the observation_bounds CTE above, so
-- the bounds have to be literals. Postgres-specific, but both the dev
-- warehouse and Neon are Postgres and the whole project is pinned to it.
date_spine as (

    select generate_series(
        (select first_observed_date from observation_bounds),
        (select last_observed_date  from observation_bounds),
        interval '1 day'
    )::date as calendar_date

),

repo_coverage_window as (

    select
        github_repo_id,
        min(repo_full_name)  as repo_full_name,
        min(category)        as category,
        min(priority)        as priority,
        min(observed_date)   as tracking_started_on
    from observations
    group by github_repo_id

),

-- What SHOULD exist: every repo, on every date from when it started being
-- tracked up to the most recent collection.
expected as (

    select
        r.github_repo_id,
        r.repo_full_name,
        r.category,
        r.priority,
        d.calendar_date
    from repo_coverage_window r
    cross join date_spine d
    where d.calendar_date >= r.tracking_started_on

),

gaps as (

    select
        e.github_repo_id,
        e.repo_full_name,
        e.category,
        e.priority,
        e.calendar_date                                 as missing_date,

        cr.run_date is not null                         as collector_ran_that_day,
        coalesce(cr.is_partial, false)                  as run_was_partial,
        coalesce(cr.is_incomplete, false)               as run_was_incomplete,
        coalesce(cr.rate_limit_hit, false)              as run_hit_rate_limit,

        case
            -- No run row at all: the cron did not fire, or died before it
            -- could record itself. Affects every repo on that date.
            when cr.run_date is null            then 'collector_did_not_run'
            -- The collector ran and stopped cleanly partway through, which is
            -- the designed behaviour at the rate-limit floor.
            when coalesce(cr.rate_limit_hit, false) then 'rate_limit_reached'
            -- It ran and reported skips or failures for other reasons.
            when coalesce(cr.is_partial, false) then 'repo_skipped_or_failed'
            -- It died mid-run without writing finished_at.
            when coalesce(cr.is_incomplete, false) then 'run_died_mid_flight'
            -- It ran, completed, reported no problems, and yet this repo has
            -- no row. That is an unexplained hole and should be looked at.
            else 'unexplained'
        end                                             as gap_reason

    from expected e
    left join observations o
      on  e.github_repo_id = o.github_repo_id
      and e.calendar_date  = o.observed_date
    left join collection_runs cr
      on  e.calendar_date  = cr.run_date
    where o.github_repo_id is null

)

select
    {{ dbt_utils.generate_surrogate_key(['github_repo_id', 'missing_date']) }} as gap_key,
    *
from gaps

-- Grain: one row per repo per change type per ISO week, above threshold.
--
-- THE HEADLINE MODEL. This is what gets rendered into digests/YYYY-WW.md.
--
-- NOT one row per event. A repo that ships three patch releases in a week is
-- one digest line reading "3 patch releases, v1.0.0 -> v1.0.3", not three
-- near-identical lines. The individual events are all still in
-- fct_change_events; this model collapses them into what a reader wants to
-- see. The real data forced this: cline/cline shipped desktop-v0.0.25 and
-- desktop-v0.0.26 on consecutive days in the first week of tracking.
--
-- Filtered above a materiality threshold and pre-ordered, so the renderer is a
-- dumb loop rather than a place where ranking logic quietly diverges from the
-- warehouse. If a change is not in this table it does not appear in the
-- digest, and the reason is always the threshold rather than a renderer bug.
--
-- The threshold is a project var so it can be lowered for a quiet week without
-- editing SQL:  dbt build --vars '{digest_min_materiality_score: 1}'

{% set min_score = var('digest_min_materiality_score', 2) %}

with events as (

    select * from {{ ref('fct_change_events') }}
    where materiality_score >= {{ min_score }}

),

-- Order events within each (week, repo, type) group so the collapse can take
-- the FIRST before-value and the LAST after-value: the chain's two ends.
sequenced as (

    select
        *,
        row_number() over (
            partition by detected_week, github_repo_id, change_type
            order by detected_at, change_key
        )                                               as seq_asc,
        row_number() over (
            partition by detected_week, github_repo_id, change_type
            order by detected_at desc, change_key desc
        )                                               as seq_desc,
        count(*) over (
            partition by detected_week, github_repo_id, change_type
        )                                               as event_count
    from events

),

-- One row per (week, repo, type). Materiality is the max in the group - they
-- are all the same type on the same repo so it is the same value, but max()
-- states the intent if that ever changes. Evidence is the LATEST event's,
-- with the chain and count added so the collapse loses nothing a reader
-- would need to verify the line.
collapsed as (

    select
        detected_week,
        detected_iso_year,
        detected_iso_week,
        github_repo_id,
        change_type,

        min(repo_full_name)                             as repo_full_name,
        min(category)                                   as category,
        min(priority)                                   as priority,
        max(materiality_score)                          as materiality_score,
        max(detected_at)                                as detected_at,
        min(event_count)                                as event_count,

        min(before_value) filter (where seq_asc = 1)    as before_value,
        min(after_value)  filter (where seq_desc = 1)   as after_value,

        (
            -- min() on the text, then cast: Postgres has no min(jsonb), and
            -- the filter picks exactly one row so min() is just "the value".
            (min(evidence) filter (where seq_desc = 1))::jsonb
            || jsonb_build_object(
                'event_count',  min(event_count),
                'first_before', min(before_value) filter (where seq_asc = 1),
                'last_after',   min(after_value)  filter (where seq_desc = 1),
                'all_after_values',
                    string_agg(after_value, ' -> ' order by detected_at, change_key)
            )
        )::text                                         as evidence

    from sequenced
    group by detected_week, detected_iso_year, detected_iso_week,
             github_repo_id, change_type

),

with_materiality_label as (

    select
        *,
        {{ materiality_label('materiality_score') }}   as materiality
    from collapsed

),

-- Coverage for the same week, so the digest can state its blind spots instead
-- of implying it saw everything. DESIGN.md section 10.
weekly_gaps as (

    select
        cast(date_trunc('week', missing_date) as date) as detected_week,
        count(*)                                       as missing_observations,
        count(distinct github_repo_id)                 as repos_with_gaps,
        count(distinct missing_date)                   as dates_with_gaps
    from {{ ref('int_collection_gaps') }}
    group by 1

),

ranked as (

    select
        e.*,

        g.missing_observations,
        g.repos_with_gaps,
        g.dates_with_gaps,

        -- Render order. Materiality first, then critical-type events ahead of
        -- routine ones at the same level, then most recent, then name so the
        -- output is deterministic and a regenerated digest does not produce a
        -- spurious diff.
        row_number() over (
            partition by e.detected_week
            order by
                e.materiality_score desc,
                case e.change_type
                    when 'archived' then 1
                    when 'license_changed' then 2
                    when 'renamed_or_transferred' then 3
                    when 'breaking_release' then 4
                    when 'major_release' then 5
                    when 'went_stale' then 6
                    else 7
                end,
                e.detected_at desc,
                e.repo_full_name asc
        ) as digest_rank

    from with_materiality_label as e
    left join weekly_gaps as g on e.detected_week = g.detected_week

)

select
    detected_iso_year,
    detected_iso_week,
    detected_week,
    digest_rank,

    github_repo_id,
    repo_full_name,
    category,
    priority,

    change_type,
    materiality,
    materiality_score,
    detected_at,
    event_count,
    before_value,
    after_value,
    evidence,

    -- Coverage caveat for the week this row belongs to. Non-null means the
    -- digest for that week must say so.
    coalesce(missing_observations, 0) as week_missing_observations,
    coalesce(repos_with_gaps, 0)      as week_repos_with_gaps,
    coalesce(dates_with_gaps, 0)      as week_dates_with_gaps

from ranked

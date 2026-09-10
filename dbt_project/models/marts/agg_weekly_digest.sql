-- Grain: one row per change event that qualifies for the weekly digest.
--
-- THE HEADLINE MODEL. This is what gets rendered into digests/YYYY-WW.md.
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

    from events as e
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
    before_value,
    after_value,
    evidence,

    -- Coverage caveat for the week this row belongs to. Non-null means the
    -- digest for that week must say so.
    coalesce(missing_observations, 0) as week_missing_observations,
    coalesce(repos_with_gaps, 0)      as week_repos_with_gaps,
    coalesce(dates_with_gaps, 0)      as week_dates_with_gaps

from ranked

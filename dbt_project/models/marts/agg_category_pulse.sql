-- Grain: one row per category per ISO week.
--
-- The top-level dashboard view: is this corner of the ecosystem speeding up or
-- slowing down. Built from the fact table plus the daily signals, so release
-- counts and star movement share the same week boundaries.
--
-- Star movement is SUMMED FROM DAILY DELTAS rather than differenced across the
-- week's endpoints. Endpoint differencing attributes a repo's whole weekly
-- movement to whichever days happen to be present, so a collection gap
-- silently inflates the figure. Summing deltas simply omits the missing days,
-- and repos_with_gaps says how much was missed.

with weeks as (

    select distinct
        cast(date_trunc('week', observed_date) as date) as pulse_week
    from {{ ref('int_activity_signals') }}

),

categories as (
    select distinct category from {{ ref('dim_repos') }}
),

-- Full grid, so a category with no activity in a week is a ZERO row rather
-- than a missing row. A line chart with holes reads as "no data", not as
-- "nothing happened", and those are different claims.
grid as (
    select w.pulse_week, c.category
    from weeks as w cross join categories as c
),

activity as (

    select
        cast(date_trunc('week', observed_date) as date)        as pulse_week,
        category,
        count(distinct github_repo_id)                         as repos_observed,
        sum(coalesce(stars_delta_day, 0))                      as stars_gained,
        count(distinct github_repo_id) filter (where is_stale) as repos_stale,
        avg(days_since_push)                                   as avg_days_since_push
    from {{ ref('int_activity_signals') }}
    group by 1, 2

),

events as (

    select
        detected_week                                               as pulse_week,
        category,
        count(*)                                                    as change_events,
        count(*) filter (where change_type in (
            'major_release', 'minor_release', 'patch_release',
            'breaking_release', 'release_unclassified'
        ))                                                          as releases,
        count(*) filter (where change_type = 'breaking_release')    as breaking_releases,
        count(*) filter (where change_type = 'went_stale')          as repos_went_stale,
        count(*) filter (where change_type = 'archived')            as repos_archived,
        count(*) filter (where materiality in ('critical', 'high')) as material_events
    from {{ ref('fct_change_events') }}
    group by 1, 2

),

gaps as (

    select
        cast(date_trunc('week', missing_date) as date) as pulse_week,
        category,
        count(*)                                       as missing_observations,
        count(distinct github_repo_id)                 as repos_with_gaps
    from {{ ref('int_collection_gaps') }}
    group by 1, 2

)

select
    g.pulse_week,
    g.category,

    coalesce(a.repos_observed, 0)        as repos_observed,
    coalesce(a.stars_gained, 0)          as stars_gained,
    coalesce(a.repos_stale, 0)           as repos_stale,
    round(a.avg_days_since_push, 1)      as avg_days_since_push,

    coalesce(e.change_events, 0)         as change_events,
    coalesce(e.releases, 0)              as releases,
    coalesce(e.breaking_releases, 0)     as breaking_releases,
    coalesce(e.repos_went_stale, 0)      as repos_went_stale,
    coalesce(e.repos_archived, 0)        as repos_archived,
    coalesce(e.material_events, 0)       as material_events,

    coalesce(gp.missing_observations, 0) as missing_observations,
    coalesce(gp.repos_with_gaps, 0)      as repos_with_gaps

from grid as g
left join activity as a on g.pulse_week = a.pulse_week and g.category = a.category
left join events as e on g.pulse_week = e.pulse_week and g.category = e.category
left join gaps as gp on g.pulse_week = gp.pulse_week and g.category = gp.category

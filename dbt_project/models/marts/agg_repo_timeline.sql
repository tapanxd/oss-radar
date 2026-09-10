-- Grain: one row per repo per calendar month.
--
-- The per-repo history strip on the dashboard, and the model that answers
-- questions a notification feed cannot answer at all: which projects went
-- quiet, when a licence changed, how a repo behaved over its tracked life.
--
-- Month-end state is taken from the LAST OBSERVATION IN THE MONTH rather than
-- from the calendar month end. A month still in progress, or one with a
-- collection gap on its final days, then reports the last thing actually seen
-- instead of a null.

with monthly_activity as (

    select distinct on (
        github_repo_id,
        cast(date_trunc('month', observed_date) as date)
    )
        github_repo_id,
        cast(date_trunc('month', observed_date) as date) as timeline_month,
        repo_full_name,
        category,
        priority,
        observed_date                                    as last_observed_in_month,
        stars                                            as stars_at_month_end,
        forks                                            as forks_at_month_end,
        open_issues                                      as open_issues_at_month_end,
        days_since_push                                  as days_since_push_at_month_end,
        is_stale                                         as is_stale_at_month_end
    from {{ ref('int_activity_signals') }}
    order by
        github_repo_id asc,
        cast(date_trunc('month', observed_date) as date),
        observed_date desc

),

monthly_deltas as (

    select
        github_repo_id,
        cast(date_trunc('month', observed_date) as date) as timeline_month,
        count(*)                                         as days_observed,
        sum(coalesce(stars_delta_day, 0))                as stars_gained_in_month
    from {{ ref('int_activity_signals') }}
    group by 1, 2

),

monthly_events as (

    select
        github_repo_id,
        detected_month                                              as timeline_month,
        count(*)                                                    as change_events,
        count(*) filter (where change_type in (
            'major_release', 'minor_release', 'patch_release',
            'breaking_release', 'release_unclassified'
        ))                                                          as releases,
        count(*) filter (where materiality in ('critical', 'high')) as material_events,
        string_agg(distinct change_type, ',' order by change_type)  as change_types
    from {{ ref('fct_change_events') }}
    group by 1, 2

),

monthly_gaps as (

    select
        github_repo_id,
        cast(date_trunc('month', missing_date) as date) as timeline_month,
        count(*)                                        as missing_observation_days
    from {{ ref('int_collection_gaps') }}
    group by 1, 2

)

select
    a.github_repo_id,
    a.timeline_month,
    a.repo_full_name,
    a.category,
    a.priority,

    a.last_observed_in_month,
    coalesce(d.days_observed, 0)            as days_observed,
    coalesce(g.missing_observation_days, 0) as missing_observation_days,

    a.stars_at_month_end,
    coalesce(d.stars_gained_in_month, 0)    as stars_gained_in_month,
    a.forks_at_month_end,
    a.open_issues_at_month_end,

    a.days_since_push_at_month_end,
    a.is_stale_at_month_end,

    coalesce(e.change_events, 0)            as change_events,
    coalesce(e.releases, 0)                 as releases,
    coalesce(e.material_events, 0)          as material_events,
    e.change_types

from monthly_activity as a
left join monthly_deltas as d
  on a.github_repo_id = d.github_repo_id and a.timeline_month = d.timeline_month
left join monthly_events as e
  on a.github_repo_id = e.github_repo_id and a.timeline_month = e.timeline_month
left join monthly_gaps as g
  on a.github_repo_id = g.github_repo_id and a.timeline_month = g.timeline_month

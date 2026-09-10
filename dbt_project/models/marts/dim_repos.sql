-- Grain: one row per tracked repo, current state.
--
-- The dimension everything joins to. Current state comes from the SCD2 spine
-- rather than from "the most recent observation", so a repo missing from
-- today's collection still shows its last known state instead of vanishing
-- from the dashboard.
--
-- Carries the pre-existing conditions that are NOT change events: a repo that
-- was already archived, or already renamed, when tracking began. Those are
-- real and worth surfacing, but they are not news and must never appear in
-- fct_change_events. This is where they live instead.

with current_state as (

    select * from {{ ref('int_repo_state_history') }}
    where is_current_state

),

latest_activity as (

    select distinct on (github_repo_id)
        github_repo_id,
        observed_date       as last_observed_date,
        stars,
        forks,
        open_issues,
        days_since_push,
        is_stale,
        days_of_history
    from {{ ref('int_activity_signals') }}
    order by github_repo_id, observed_date desc

),

change_counts as (

    select
        github_repo_id,
        count(*)                                                    as total_change_events,
        count(*) filter (where materiality in ('critical','high'))  as material_change_events,
        max(detected_at)                                            as last_change_detected_at
    from {{ ref('int_change_events') }}
    group by github_repo_id

),

gap_counts as (

    select
        github_repo_id,
        count(*) as missing_observation_days
    from {{ ref('int_collection_gaps') }}
    group by github_repo_id

)

select
    s.github_repo_id,
    s.repo_full_name,
    s.api_full_name,
    s.repo_owner,
    s.repo_name,
    s.category,
    s.priority,

    s.license_spdx,
    s.is_archived,
    s.is_fork,
    s.default_branch,
    s.description,
    s.homepage,
    s.primary_language,
    s.topics,

    s.repo_created_at,
    s.repo_pushed_at,

    a.stars,
    a.forks,
    a.open_issues,
    a.days_since_push,
    coalesce(a.is_stale, false)                     as is_stale,

    s.valid_from                                    as current_state_since,
    a.last_observed_date,
    coalesce(a.days_of_history, 0)                  as days_tracked,

    coalesce(c.total_change_events, 0)              as total_change_events,
    coalesce(c.material_change_events, 0)           as material_change_events,
    c.last_change_detected_at,
    coalesce(g.missing_observation_days, 0)         as missing_observation_days,

    -- PRE-EXISTING CONDITIONS, not change events.
    --
    -- A repo already renamed or already archived when collection started
    -- produces no event, because nothing changed during the window. That is
    -- correct for the digest and wrong for the dashboard, where "this repo you
    -- track now lives somewhere else" is worth knowing regardless of when it
    -- happened. Surfaced here so the fact is not lost.
    (s.repo_full_name is distinct from s.api_full_name) as is_renamed_from_config,
    (s.is_archived and s.state_sequence = 1)            as was_archived_before_tracking

from current_state s
left join latest_activity a on s.github_repo_id = a.github_repo_id
left join change_counts   c on s.github_repo_id = c.github_repo_id
left join gap_counts      g on s.github_repo_id = g.github_repo_id

-- THE UNION. Every detected change, normalised to one shape and ranked.
--
-- Grain: one row per detected change event.
--
-- Four upstream detectors feed this model. They deliberately have different
-- internal shapes - a licence diff and a semver bump have little in common -
-- and this is the single place they are reconciled, so that fct_change_events
-- and the digest never need to know which detector produced a row.
--
--   int_metadata_changes   licence, rename, archive, branch, description...
--   int_release_events     major / minor / patch / breaking / unclassified
--   int_activity_signals   star_spike and went_stale, derived here as
--                          TRANSITIONS rather than as standing states
--
-- WHY THE ACTIVITY EVENTS ARE DERIVED HERE
--
-- int_activity_signals is a daily measurement table: it says "this repo is
-- stale today", which is true again tomorrow, and the day after. Emitting that
-- as an event every day would bury the digest. What is newsworthy is the
-- MOMENT it became true, so the false-to-true transition is detected here and
-- the standing state is left in the signals table where it belongs.
--
-- RANKING is applied last, from macros/materiality.sql, so the weighting is
-- legible in one file rather than smeared across the detectors.

with metadata_changes as (

    select
        change_key,
        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        change_type,
        base_materiality,
        before_value,
        after_value,
        evidence,
        'int_metadata_changes' as source_model
    from {{ ref('int_metadata_changes') }}

),

release_events as (

    select
        change_key,
        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        change_type,
        base_materiality,
        before_value,
        after_value,
        evidence,
        'int_release_events' as source_model
    from {{ ref('int_release_events') }}

),

-- Standing states plus their previous value, so transitions can be picked out.
activity_with_previous as (

    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date,
        stars,
        is_stale,
        is_star_spike,
        days_since_push,
        stars_per_day_recent,
        stars_per_day_baseline,
        days_of_history,

        lag(is_stale) over (
            partition by github_repo_id order by observed_date
        ) as previous_is_stale,

        lag(is_star_spike) over (
            partition by github_repo_id order by observed_date
        ) as previous_is_star_spike

    from {{ ref('int_activity_signals') }}

),

went_stale_events as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'github_repo_id', 'observed_date', "'went_stale'"
        ]) }}                  as change_key,
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date          as detected_at,
        'went_stale'           as change_type,
        'medium'               as base_materiality,
        'active'               as before_value,
        'stale'                as after_value,
        json_build_object(
            'days_since_push', days_since_push,
            'threshold_days', 180,
            'detected_at', observed_date
        )::text                as evidence,
        'int_activity_signals' as source_model
    from activity_with_previous
    -- previous_is_stale must be explicitly false, not merely "not true". On a
    -- repo's first observation the lag is NULL, and treating NULL as false
    -- would report every already-stale repo as having just gone stale.
    where is_stale and previous_is_stale = false

),

star_spike_events as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'github_repo_id', 'observed_date', "'star_spike'"
        ]) }}                        as change_key,
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date                as detected_at,
        'star_spike'                 as change_type,
        'low'                        as base_materiality,
        stars_per_day_baseline::text as before_value,
        stars_per_day_recent::text   as after_value,
        json_build_object(
            'stars', stars,
            'stars_per_day_recent', stars_per_day_recent,
            'stars_per_day_baseline', stars_per_day_baseline,
            'multiple', round(
                                          stars_per_day_recent
                                          / nullif(stars_per_day_baseline, 0), 2
                                      ),
            'days_of_history', days_of_history,
            'detected_at', observed_date
        )::text                      as evidence,
        'int_activity_signals'       as source_model
    from activity_with_previous
    where is_star_spike and previous_is_star_spike = false

),

unioned as (

    select * from metadata_changes
    union all
    select * from release_events
    union all
    select * from went_stale_events
    union all
    select * from star_spike_events

),

-- Push recency at the time of detection, for the abandonment demotion. Left
-- join: a change detected on a date with no activity row must not vanish.
with_activity_context as (

    select
        u.*,
        a.days_since_push
    from unioned as u
    left join {{ ref('int_activity_signals') }} as a
      on u.github_repo_id = a.github_repo_id
      and u.detected_at = a.observed_date

),

ranked as (

    select
        *,

        {{ materiality_score('base_materiality') }}  as base_score,

        case when priority = 'high' then 1 else 0 end as priority_adjustment,

        case
            when days_since_push >= {{ abandoned_after_days() }} then -1
            else 0
        end                                           as abandonment_adjustment

    from with_activity_context

),

final as (

    select
        change_key,
        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        change_type,
        source_model,

        before_value,
        after_value,
        evidence,

        base_materiality,
        base_score,
        priority_adjustment,
        abandonment_adjustment,

        -- Clamped to 1-4 so a high-priority repo being archived cannot
        -- overflow past critical, and a demoted patch release cannot fall
        -- below low.
        greatest(1, least(4,
            base_score + priority_adjustment + abandonment_adjustment
        ))   as materiality_score,

        {{ materiality_label(
            'greatest(1, least(4, base_score + priority_adjustment + abandonment_adjustment))'
        ) }}                                         as materiality,

        days_since_push

    from ranked

)

select * from final

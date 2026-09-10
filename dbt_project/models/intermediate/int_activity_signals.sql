-- Grain: one row per repo per observed date.
--
-- Activity metrics that are about RATE rather than about a discrete change:
-- star velocity, days since the last push, and whether the repo currently
-- reads as stale. int_change_events turns transitions in these flags into
-- events; this model just measures.
--
-- THE HONESTY PROBLEM THIS MODEL EXISTS TO SOLVE
--
-- Velocity needs history, and history accrues in real time - the collector
-- started on 2026-09-09 and nothing can backfill it. A naive implementation
-- computes a "7-day velocity" from two days of data, compares it against a
-- "90-day mean" built from the same two days, and confidently reports a star
-- spike for every repo on the planet. That is worse than reporting nothing,
-- because it looks like signal.
--
-- So every windowed metric is paired with a sufficiency flag, and no spike is
-- ever asserted while the window is underfilled. DESIGN.md section 10 requires
-- the digest to state its blind spots rather than imply continuous coverage.
--
-- days_since_push is deliberately NOT gated: repo_pushed_at is an absolute
-- timestamp from the API, so staleness is knowable from the very first
-- observation without any accumulated history at all.

{% set velocity_window_days = 7 %}
{% set baseline_window_days = 90 %}
{% set spike_multiple = 3 %}
{% set stale_after_days = 180 %}

with observations as (

    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date,
        stars,
        forks,
        open_issues,
        repo_pushed_at
    from {{ ref('stg_repo_observations') }}

),

with_history_depth as (

    select
        *,

        -- How much history this repo actually has, as of this row. Everything
        -- below keys its sufficiency checks off this rather than off the
        -- project-wide date range, because a repo added to repos.yml later has
        -- less history than the rest even though the pipeline has been running
        -- for months.
        count(*) over (
            partition by github_repo_id
            order by observed_date
            rows between unbounded preceding and current row
        ) as days_of_history,

        lag(stars) over (
            partition by github_repo_id order by observed_date
        ) as previous_stars,

        -- Stars as of N days ago, by value rather than by row offset, so a
        -- collection gap does not silently shorten the window.
        first_value(stars) over (
            partition by github_repo_id
            order by observed_date
            range between interval '{{ velocity_window_days }} days' preceding and current row
        ) as stars_at_velocity_window_start,

        first_value(stars) over (
            partition by github_repo_id
            order by observed_date
            range between interval '{{ baseline_window_days }} days' preceding and current row
        ) as stars_at_baseline_window_start,

        min(observed_date) over (
            partition by github_repo_id
            order by observed_date
            range between interval '{{ velocity_window_days }} days' preceding and current row
        ) as velocity_window_start_date,

        min(observed_date) over (
            partition by github_repo_id
            order by observed_date
            range between interval '{{ baseline_window_days }} days' preceding and current row
        ) as baseline_window_start_date

    from observations

),

metrics as (

    select
        *,

        stars - previous_stars                          as stars_delta_day,

        (observed_date - repo_pushed_at::date)          as days_since_push,

        -- Sufficiency. A window is only trusted once it is actually filled.
        (days_of_history >= {{ velocity_window_days }}) as has_velocity_history,
        (days_of_history >= {{ baseline_window_days }}) as has_baseline_history,

        nullif(observed_date - velocity_window_start_date, 0) as velocity_window_days_actual,
        nullif(observed_date - baseline_window_start_date, 0) as baseline_window_days_actual

    from with_history_depth

),

rates as (

    select
        *,

        -- Mean stars per day across each window. NULL rather than 0 when the
        -- window is not yet filled: 0 would read as "no growth", which is a
        -- claim, whereas NULL correctly says "not known yet".
        case
            when has_velocity_history
            then (stars - stars_at_velocity_window_start)::numeric
                 / velocity_window_days_actual
        end                                             as stars_per_day_recent,

        case
            when has_baseline_history
            then (stars - stars_at_baseline_window_start)::numeric
                 / baseline_window_days_actual
        end                                             as stars_per_day_baseline

    from metrics

),

final as (

    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date,

        stars,
        forks,
        open_issues,
        stars_delta_day,
        days_of_history,

        round(stars_per_day_recent, 3)                  as stars_per_day_recent,
        round(stars_per_day_baseline, 3)                as stars_per_day_baseline,

        has_velocity_history,
        has_baseline_history,

        -- Both windows must be filled AND the baseline must be non-trivial.
        -- A repo that gained 1 star/day historically and now gains 4 is not
        -- news; the multiple is huge but the absolute movement is noise. The
        -- README states plainly that this threshold is judgement, not a
        -- validated model.
        (
            has_velocity_history
            and has_baseline_history
            and stars_per_day_baseline > 0
            and stars_per_day_recent > {{ spike_multiple }} * stars_per_day_baseline
        )                                               as is_star_spike,

        repo_pushed_at,
        days_since_push,
        (days_since_push >= {{ stale_after_days }})     as is_stale,

        -- Carried so downstream models and the digest can say WHY a signal is
        -- absent instead of just omitting the repo.
        case
            when not has_velocity_history
            then 'insufficient history: ' || days_of_history
                 || ' of {{ velocity_window_days }} days'
            when not has_baseline_history
            then 'no baseline: ' || days_of_history
                 || ' of {{ baseline_window_days }} days'
        end                                             as velocity_caveat

    from rates

)

select * from final

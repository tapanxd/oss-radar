-- Grain: one row per detected change event.
--
-- The core fact table. Everything the digest and the dashboard read comes from
-- here or from an aggregate built on it.
--
-- Adds the calendar buckets the aggregates group by, so week and month
-- boundaries are defined once here rather than being recomputed - and
-- potentially differently - in each agg model.
--
-- Weeks are ISO weeks starting Monday. date_trunc('week') in Postgres is
-- already ISO, so digest filenames of the form YYYY-WW line up with what
-- anyone else calling it "week 37" means.

with events as (

    select * from {{ ref('int_change_events') }}

)

select
    change_key,
    github_repo_id,
    repo_full_name,
    category,
    priority,

    detected_at,
    cast(date_trunc('week',  detected_at) as date)  as detected_week,
    cast(date_trunc('month', detected_at) as date)  as detected_month,
    cast(extract(isoyear from detected_at) as integer) as detected_iso_year,
    cast(extract(week    from detected_at) as integer) as detected_iso_week,

    change_type,
    source_model,

    before_value,
    after_value,
    evidence,

    base_materiality,
    materiality,
    cast(materiality_score as integer)              as materiality_score,
    cast(priority_adjustment as integer)            as priority_adjustment,
    cast(abandonment_adjustment as integer)         as abandonment_adjustment,

    cast(days_since_push as integer)                as days_since_push

from events

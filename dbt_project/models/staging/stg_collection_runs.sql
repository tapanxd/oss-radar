-- Cast and rename only.

with source as (

    select * from {{ source('raw', 'collection_runs') }}

),

renamed as (

    select
        run_id,
        started_at,
        finished_at,
        collector_version,

        repos_attempted,
        repos_succeeded,
        repos_failed,
        repos_skipped,

        rate_limit_hit,
        rate_limit_remaining,
        notes,

        -- Derived here rather than downstream because it is a pure restatement
        -- of columns already present, not logic.
        (started_at at time zone 'UTC')::date as run_date,
        finished_at is null                   as is_incomplete,
        repos_failed > 0 or repos_skipped > 0 as is_partial

    from source

)

select * from renamed

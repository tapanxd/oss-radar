-- Cast and rename only. No logic, no filtering, no dedup: the source is
-- already unique on (repo_full_name, observed_date) and hiding a violation
-- here would mask a collector bug that the source test is there to catch.

with source as (

    select * from {{ source('raw', 'repo_observations') }}

),

renamed as (

    select
        id                                          as observation_id,

        -- identity and config
        repo_full_name,
        split_part(repo_full_name, '/', 1)          as repo_owner,
        split_part(repo_full_name, '/', 2)          as repo_name,
        category,
        priority,

        -- collection metadata
        observed_at,
        observed_date,
        collector_version,

        -- diffed attributes
        stars,
        forks,
        open_issues,
        subscribers,
        license_spdx,
        is_archived,
        is_disabled,
        is_fork,
        default_branch,
        description,
        homepage,
        topics,
        primary_language,
        size_kb,

        repo_created_at,
        repo_pushed_at,
        repo_updated_at,

        -- latest release; null where the repo has never cut one
        latest_release_tag,
        latest_release_name,
        latest_release_published_at,
        latest_release_is_prerelease,
        latest_release_body,
        latest_release_body_sha256,

        -- payloads kept addressable but not unpacked here; unpacking belongs
        -- in intermediate, where there is a reason to reach into them
        raw_repo_payload,
        raw_release_payload

    from source

)

select * from renamed

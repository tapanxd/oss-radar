-- Cast and rename only. No logic, no filtering, no dedup: the source is
-- already unique on (repo_full_name, observed_date) and hiding a violation
-- here would mask a collector bug that the source test is there to catch.

with source as (

    select * from {{ source('raw', 'repo_observations') }}

),

renamed as (

    select
        id                                                            as observation_id,

        -- IDENTITY
        --
        -- Three different things, and conflating them causes real bugs:
        --
        --   github_repo_id  GitHub's numeric id. Stable across renames and
        --                   transfers, so it is the correct key to build state
        --                   history on. Partitioning by name instead would make
        --                   a rename look like one repo dying and another being
        --                   born.
        --   repo_full_name  What repos.yml asked for. The collector stores the
        --                   REQUESTED path (collect.py:222), not what the API
        --                   returned, so this is a stable config key.
        --   api_full_name   What the API actually returned. GitHub transparently
        --                   redirects renamed repos, so this differing from
        --                   repo_full_name IS the rename signal.
        (raw_repo_payload ->> 'id')::bigint                           as github_repo_id,
        repo_full_name,
        raw_repo_payload ->> 'full_name'                              as api_full_name,
        split_part(repo_full_name, '/', 1)                            as repo_owner,
        split_part(repo_full_name, '/', 2)                            as repo_name,
        split_part(raw_repo_payload ->> 'full_name', '/', 1)          as api_repo_owner,
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
        -- Normalised to a sorted comma-separated string rather than left as
        -- text[]. Two reasons:
        --
        --   1. CORRECTNESS. GitHub does not guarantee topic order, so hashing
        --      the raw array would start a new state period whenever the same
        --      topics came back in a different order - a stream of change
        --      events describing nothing.
        --   2. TESTABILITY. dbt unit tests cannot cast a Postgres array type
        --      (it renders as `cast(null as ARRAY)`), so an array column here
        --      makes every downstream unit test unrunnable.
        --
        -- Nothing downstream needs array semantics; topics are only ever
        -- compared and displayed.
        array_to_string(array(select unnest(topics) order by 1), ',') as topics,
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

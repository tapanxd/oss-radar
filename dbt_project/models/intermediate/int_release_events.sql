-- One row per newly observed release per repo, with the tag parsed and the
-- bump classified.
--
-- Built directly from the observation log rather than from the SCD2 spine.
-- Release fields are deliberately excluded from the spine's tracked state
-- (they would start a new state period on every release), so this model does
-- its own consecutive-day comparison on latest_release_tag.
--
-- A repo's FIRST observation never produces a release event. The tag that was
-- already current when tracking began is not news, and reporting it would make
-- the first digest claim 40-odd repos had all just released.
--
-- UNPARSEABLE TAGS ARE NOT DROPPED. A tag that does not yield a semver core
-- becomes release_unclassified at medium materiality. Silently discarding it
-- would mean a project with unusual tagging simply vanishes from the digest,
-- which is worse than an imprecise entry. DESIGN.md section 6.

with observations as (

    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date,
        latest_release_tag,
        latest_release_name,
        latest_release_published_at,
        latest_release_is_prerelease,
        latest_release_body
    from {{ ref('stg_repo_observations') }}

),

sequenced as (

    select
        *,
        row_number() over (
            partition by github_repo_id order by observed_date
        ) as observation_sequence,
        lag(latest_release_tag) over (
            partition by github_repo_id order by observed_date
        ) as previous_tag
    from observations

),

new_releases as (

    select *
    from sequenced
    where latest_release_tag is not null
      -- Skip the first observation: there is no prior tag to compare against.
      and observation_sequence > 1
      and latest_release_tag is distinct from previous_tag

),

parsed as (

    select
        *,

        {{ semver_part('latest_release_tag', 1) }} as major,
        {{ semver_part('latest_release_tag', 2) }} as minor,
        {{ semver_part('latest_release_tag', 3) }} as patch,

        {{ semver_part('previous_tag', 1) }}       as previous_major,
        {{ semver_part('previous_tag', 2) }}       as previous_minor,
        {{ semver_part('previous_tag', 3) }}       as previous_patch,

        {{ is_calver('latest_release_tag') }}      as is_calver_tag,
        {{ semver_prerelease('latest_release_tag') }} as prerelease_label

    from new_releases

),

-- Which breaking-change markers fired, and the text that matched. Aggregated
-- so a release with several markers produces one row, not one per marker.
breaking_matches as (

    select
        p.github_repo_id,
        p.observed_date,
        p.latest_release_tag,
        count(*)                                                   as markers_matched,
        string_agg(distinct m.marker_id, ',' order by m.marker_id) as matched_marker_ids,
        -- The matched LINE, not the matched capture group. This is the
        -- evidence a reader needs to judge the call without opening the
        -- release page, so it has to be a readable sentence.
        --
        -- The pattern is wrapped in a non-capturing group inside our own
        -- capture group, and padded with [^\n]* on both sides, so the whole
        -- surrounding line comes back. Taking (regexp_match(...))[1] on the
        -- bare pattern instead returns the pattern's OWN first group, which
        -- for `no longer (supported|works|available)` is the useless word
        -- "available" and for the heading patterns is a bare newline.
        left(
            btrim(
                min(
                    (regexp_match(
                        p.latest_release_body,
                        '([^\n]*(?:' || m.pattern || ')[^\n]*)',
                        'i'
                    ))[1]
                )
            ),
            300
        )                                                          as matched_text
    from parsed as p
    inner join {{ ref('breaking_change_markers') }} as m
      on p.latest_release_body is not null
     and p.latest_release_body ~* m.pattern
    group by p.github_repo_id, p.observed_date, p.latest_release_tag

),

classified as (

    select
        p.*,

        coalesce(b.markers_matched, 0) as breaking_markers_matched,
        b.matched_marker_ids,
        b.matched_text                 as breaking_evidence_text,
        b.markers_matched is not null  as has_breaking_markers,

        case
            -- Order matters. Breaking markers outrank the numeric bump: a
            -- project that ships a breaking change in a minor release is
            -- exactly the case a digest exists to catch.
            when b.markers_matched is not null then 'breaking_release'
            when p.major is null then 'release_unclassified'
            when p.previous_major is null then 'release_unclassified'
            when p.major > p.previous_major then 'major_release'
            when p.major = p.previous_major
             and p.minor > p.previous_minor then 'minor_release'
            when p.major = p.previous_major
             and p.minor = p.previous_minor
             and p.patch > p.previous_patch then 'patch_release'
            -- A version that went backwards is not a bump. It usually means
            -- the repo publishes releases for several components under one
            -- tag namespace, so the "latest" flips between them.
            else 'release_unclassified'
        end                            as change_type

    from parsed as p
    left join breaking_matches as b
      on p.github_repo_id = b.github_repo_id
      and p.observed_date = b.observed_date
      and p.latest_release_tag = b.latest_release_tag

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'github_repo_id', 'latest_release_tag'
        ]) }}                                         as change_key,

        github_repo_id,
        repo_full_name,
        category,
        priority,
        observed_date                                 as detected_at,

        change_type,

        case change_type
            when 'breaking_release' then 'high'
            when 'major_release' then 'high'
            when 'minor_release' then 'medium'
            when 'release_unclassified' then 'medium'
            when 'patch_release' then 'low'
        end                                           as base_materiality,

        previous_tag                                  as before_value,
        latest_release_tag                            as after_value,

        latest_release_name                           as release_name,
        latest_release_published_at                   as release_published_at,
        coalesce(latest_release_is_prerelease, false) as is_github_prerelease,
        prerelease_label,
        is_calver_tag,

        major, minor, patch,
        previous_major, previous_minor, previous_patch,

        has_breaking_markers,
        breaking_markers_matched,
        matched_marker_ids,

        json_build_object(
            'previous_tag', previous_tag,
            'new_tag', latest_release_tag,
            'published_at', latest_release_published_at,
            'breaking_markers', matched_marker_ids,
            'matched_text', breaking_evidence_text,
            'detected_at', observed_date
        )::text                                       as evidence

    from classified

)

select * from final

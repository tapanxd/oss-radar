-- Unpivots the SCD2 spine into one row per repo per changed attribute.
--
-- int_repo_state_history says "the repo looked like this from X to Y". This
-- model answers "what specifically changed, and what was it before" by
-- comparing each state period against the one before it.
--
-- The attribute list below is the single place change types and their base
-- materiality are declared. Adding a tracked attribute means adding one entry
-- here, not editing SQL in three places. Keep it in sync with the
-- accepted_values test on change_type.
--
-- A repo's FIRST state period is skipped: there is nothing before it to
-- compare against. Something already true when tracking began is not news, and
-- reporting it as a change would make the first digest a wall of false events.
-- dim_repos surfaces that pre-existing state separately.

{% set tracked_attributes = [
    {'column': 'api_full_name',   'change_type': 'renamed_or_transferred',    'materiality': 'critical'},
    {'column': 'license_spdx',    'change_type': 'license_changed',           'materiality': 'critical'},
    {'column': 'default_branch',  'change_type': 'default_branch_changed',    'materiality': 'medium'},
    {'column': 'description',     'change_type': 'description_changed',       'materiality': 'low'},
    {'column': 'homepage',        'change_type': 'homepage_changed',          'materiality': 'low'},
    {'column': 'topics',          'change_type': 'topics_changed',            'materiality': 'low'},
    {'column': 'category',        'change_type': 'tracking_category_changed', 'materiality': 'low'},
    {'column': 'priority',        'change_type': 'tracking_priority_changed', 'materiality': 'low'}
] %}

with state_history as (

    select * from {{ ref('int_repo_state_history') }}

),

with_previous as (

    select
        github_repo_id,
        state_sequence,
        repo_full_name,
        valid_from as detected_at,
        is_initial_state,

        -- category and priority are not listed here: they are in the tracked
        -- attribute loop below, which already emits them alongside their lag.
        -- Selecting them twice makes every downstream reference ambiguous.

        {% for attr in tracked_attributes %}
        {{ attr.column }},
        lag({{ attr.column }}) over (
            partition by github_repo_id order by state_sequence
        )          as previous_{{ attr.column }},
        {% endfor %}

        is_archived,
        lag(is_archived) over (
            partition by github_repo_id order by state_sequence
        )          as previous_is_archived

    from state_history

),

-- One SELECT per tracked attribute, unioned. Generated rather than written out
-- so the attribute list above stays the only source of truth.
attribute_changes as (

    {% for attr in tracked_attributes %}
    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        state_sequence,
        '{{ attr.change_type }}'         as change_type,
        '{{ attr.materiality }}'         as base_materiality,
        '{{ attr.column }}'              as changed_attribute,
        previous_{{ attr.column }}::text as before_value,
        {{ attr.column }}::text          as after_value
    from with_previous
    where not is_initial_state
      -- `is distinct from`, not `<>`: with plain inequality a NULL on either
      -- side yields NULL rather than true, so a licence appearing out of
      -- nothing, or disappearing, would never be reported.
      and {{ attr.column }} is distinct from previous_{{ attr.column }}

    union all
    {% endfor %}

    -- is_archived is handled separately from the loop because DIRECTION
    -- matters. Archiving is the single highest-materiality event this project
    -- reports; un-archiving is a different, rarer event and must not be
    -- flattened into the same change type with the values simply swapped.
    select
        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        state_sequence,
        case when is_archived then 'archived' else 'unarchived' end as change_type,
        case when is_archived then 'critical' else 'medium' end     as base_materiality,
        'is_archived'                                               as changed_attribute,
        previous_is_archived::text                                  as before_value,
        is_archived::text                                           as after_value
    from with_previous
    where not is_initial_state
      and is_archived is distinct from previous_is_archived

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'github_repo_id', 'state_sequence', 'change_type'
        ]) }}   as change_key,

        github_repo_id,
        repo_full_name,
        category,
        priority,
        detected_at,
        change_type,
        base_materiality,
        changed_attribute,
        before_value,
        after_value,

        -- EVIDENCE. Non-negotiable per DESIGN.md section 6: a digest entry the
        -- reader cannot verify without opening GitHub adds nothing over a
        -- notification. For a metadata change the evidence IS the before and
        -- after pair, so it is assembled here rather than left to the digest
        -- renderer to reconstruct.
        json_build_object(
            'attribute', changed_attribute,
            'before', before_value,
            'after', after_value,
            'detected_at', detected_at
        )::text as evidence

    from attribute_changes

)

select * from final

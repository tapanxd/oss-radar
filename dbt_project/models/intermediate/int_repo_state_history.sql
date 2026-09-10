-- THE SCD2 SPINE. Everything downstream depends on this model.
--
-- Collapses consecutive identical daily observations into validity ranges:
-- one row per repo per distinct state period, with valid_from / valid_to.
-- Same output shape as a dbt snapshot, built from the observation log instead.
--
-- Why not `dbt snapshot` (DESIGN.md section 5): a snapshot can only capture
-- change from the moment it first runs, and a missed run loses a change
-- permanently with no way to backfill. This model is a pure function of the
-- observation log, so it can be dropped and rebuilt at any time and is
-- identical every time. That property is the whole argument.
--
-- TWO DELIBERATE EXCLUSIONS from the tracked state:
--
--   Volatile counts (stars, forks, open_issues, subscribers) change almost
--   every day. Including them would start a new state period daily and make
--   this table a verbose copy of the observation log rather than a history of
--   meaningful change. They are handled as velocity in int_activity_signals.
--
--   Release fields are handled by int_release_events, which has to parse
--   semver anyway. Including latest_release_tag here would emit a duplicate
--   "something changed" event for every release.
--
-- Partitioned on github_repo_id, NOT on name: the id survives renames and
-- transfers, so a rename shows up as an attribute change within one repo's
-- history rather than as one repo disappearing and another appearing.

with observations as (

    select * from {{ ref('stg_repo_observations') }}

),

hashed as (

    select
        *,

        -- Fingerprint of everything treated as state. Any difference between
        -- consecutive days starts a new state period.
        --
        -- generate_surrogate_key is used rather than a hand-rolled md5 because
        -- it maps NULL to a sentinel string. A plain concatenation makes
        -- NULL || 'x' collapse to NULL, so a license going from null to MIT
        -- would hash identically to a license going from null to Apache-2.0.
        {{ dbt_utils.generate_surrogate_key([
            'api_full_name',
            'license_spdx',
            'is_archived',
            'is_disabled',
            'is_fork',
            'default_branch',
            'description',
            'homepage',
            'primary_language',
            'category',
            'priority',
            'topics' 
        ]) }} as state_hash

    from observations

),

-- The lag is taken in its own step rather than inline in the comparison
-- below. It reads better, and it keeps the window function out of an
-- `is distinct from` expression, which sqlfluff's Postgres dialect cannot
-- parse.
with_previous_hash as (

    select
        *,
        lag(state_hash) over (
            partition by github_repo_id
            order by observed_date
        ) as previous_state_hash

    from hashed

),

marked as (

    select
        *,

        -- `is distinct from` rather than `<>`. With plain inequality a NULL on
        -- either side yields NULL, not true, so the very first observation of
        -- a repo (where the lag is NULL) would not be marked as a new state
        -- and the repo would be silently missing from its own history.
        --
        -- Cast rather than wrapped in a CASE: it says the same thing in one
        -- line, and sqlfluff's Postgres dialect cannot parse `is distinct
        -- from` inside a CASE expression.
        (state_hash is distinct from previous_state_hash)::int as starts_new_state

    from with_previous_hash

),

numbered as (

    select
        *,
        sum(starts_new_state) over (
            partition by github_repo_id
            order by observed_date
            rows between unbounded preceding and current row
        ) as state_sequence

    from marked

),

collapsed as (

    select
        github_repo_id,
        state_sequence,

        min(observed_date)                              as valid_from,
        max(observed_date)                              as last_observed_date,
        count(*)                                        as days_observed_in_state,

        -- Attributes are constant within a state period by construction, so
        -- min() is just "the value" and avoids needing a DISTINCT ON.
        min(repo_full_name)                             as repo_full_name,
        min(api_full_name)                              as api_full_name,
        min(repo_owner)                                 as repo_owner,
        min(repo_name)                                  as repo_name,
        min(api_repo_owner)                             as api_repo_owner,
        min(category)                                   as category,
        min(priority)                                   as priority,
        min(license_spdx)                               as license_spdx,
        bool_or(is_archived)                            as is_archived,
        bool_or(is_disabled)                            as is_disabled,
        bool_or(is_fork)                                as is_fork,
        min(default_branch)                             as default_branch,
        min(description)                                as description,
        min(homepage)                                   as homepage,
        min(primary_language)                           as primary_language,
        min(topics)                                     as topics,
        min(state_hash)                                 as state_hash,

        -- Carried through so downstream models can reason about the repo
        -- without rejoining the observation log.
        min(repo_created_at)                            as repo_created_at,
        max(repo_pushed_at)                             as repo_pushed_at

    from numbered
    group by github_repo_id, state_sequence

),

with_validity as (

    select
        *,

        -- The day before the next state began. NULL means "still current".
        --
        -- Derived from the next period's valid_from rather than from
        -- last_observed_date + 1, so a collection gap does not manufacture a
        -- phantom period where the repo had no state. Gaps are a separate
        -- concern and are reported by int_collection_gaps.
        lead(valid_from) over (
            partition by github_repo_id
            order by valid_from
        ) - 1 as valid_to,

        lead(valid_from) over (
            partition by github_repo_id
            order by valid_from
        ) is null as is_current_state,

        -- A repo whose first observed state is also its only state has no
        -- detected change; the distinction matters when reporting whether
        -- something changed during the window or was already that way when
        -- tracking began.
        state_sequence = 1 as is_initial_state

    from collapsed

)

select
    {{ dbt_utils.generate_surrogate_key(['github_repo_id', 'state_sequence']) }} as repo_state_key,
    *
from with_validity

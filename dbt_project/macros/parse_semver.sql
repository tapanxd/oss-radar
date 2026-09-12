{#
  Extracts a semver core (major.minor.patch) out of a real-world GitHub tag.

  Kept as a macro rather than inlined so the same expression is used by the
  model and by anything that tests it, and so the regex lives in exactly one
  place.

  Every shape below was pulled from an actual dry run against the tracked
  repos, not invented:

      v1.12.4                        conventional
      3.3.1                          unprefixed
      desktop-v0.0.25                component prefix
      rust-v0.153.4                  language prefix
      python-v4.2.0                  language prefix
      arize-phoenix-client-v3.5.0    long product prefix
      autogpt-platform-beta-v0.7.4   prefix containing a channel word
      code-scan-action-0.2.0         prefix, no v
      @arizeai/phoenix-evals@2.5.0   scoped npm package
      @upstash/context7-mcp@4.0.7    scoped npm package
      langchain-core==1.6.2          python requirement pin
      sdk==0.4.4                     python requirement pin
      v2.0.0-vscode                  suffix that is NOT a semver prerelease
      2026.8.31                      CalVer, dotted
      2026-07-28                     CalVer, dashed

  The regex deliberately anchors on the LAST occurrence of a digit triple, so a
  prefix containing digits does not win over the real version. CalVer is not
  matched as semver on purpose - `2026.8.31` parsed as major=2026 would make
  every date-versioned release look like a major bump, which is exactly the
  false-positive this project cannot afford. Those fall through to
  release_unclassified rather than being silently dropped (DESIGN.md section 6).
#}

{% macro semver_part(tag_column, part) %}
    {#- part: 1 = major, 2 = minor, 3 = patch -#}
    (
        case
            -- CalVer guard, checked FIRST. A four-digit leading group that
            -- looks like a year is not a semver major, however much it parses
            -- like one.
            when {{ tag_column }} ~ '(^|[^0-9])(19|20)[0-9]{2}[.\-][0-9]{1,2}[.\-][0-9]{1,2}($|[^0-9])'
                then null
            else nullif(
                (regexp_match(
                    {{ tag_column }},
                    '([0-9]+)\.([0-9]+)\.([0-9]+)(?!.*[0-9]+\.[0-9]+\.[0-9]+)'
                ))[{{ part }}],
                ''
            )::int
        end
    )
{% endmacro %}


{% macro is_calver(tag_column) %}
    (
        {{ tag_column }} ~ '(^|[^0-9])(19|20)[0-9]{2}[.\-][0-9]{1,2}[.\-][0-9]{1,2}($|[^0-9])'
    )
{% endmacro %}


{#
  A semver prerelease is a hyphen-suffix on the version itself, per the spec:
  1.0.0-rc.1, 1.0.0-beta. It is NOT any hyphen anywhere in the tag - a tag like
  `desktop-v0.0.25` has a hyphen in its PREFIX and is a normal release, and
  `v2.0.0-vscode` is a platform-specific build rather than a prerelease. Both
  are real tags from the tracked set and both would be misclassified by a naive
  "contains a hyphen" check.
#}
{% macro semver_prerelease(tag_column) %}
    nullif(
        (regexp_match(
            {{ tag_column }},
            '[0-9]+\.[0-9]+\.[0-9]+-((?:alpha|beta|rc|pre|dev|canary|next|snapshot)[0-9A-Za-z.\-]*)'
        ))[1],
        ''
    )
{% endmacro %}


{#
  Everything BEFORE the version core: the component or package name a monorepo
  puts in front of its tags. `desktop-v0.0.25` -> `desktop-`,
  `@arizeai/phoenix-evals@2.5.0` -> `@arizeai/phoenix-evals@`, `v1.2.3` -> ``.
  An optional leading v is stripped with the version, so a project that drops
  or adds the v between releases still compares as the same line.

  Two consecutive "latest" tags with different prefixes are two different
  components, and their version numbers have nothing to do with each other -
  `@arizeai/phoenix-evals@2.5.0` -> `arize-phoenix-v20.10.0` is not an
  eighteen-major bump. The same last-triple anchoring as semver_part, so a
  digit in the prefix does not split it.
#}
{% macro semver_prefix(tag_column) %}
    regexp_replace(
        {{ tag_column }},
        '[vV]?[0-9]+\.[0-9]+\.[0-9]+(?!.*[0-9]+\.[0-9]+\.[0-9]+).*$',
        ''
    )
{% endmacro %}

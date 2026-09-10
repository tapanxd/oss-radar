{#
  Materiality ranking, in ONE place.

  DESIGN.md section 6 fixes the rule: base materiality comes from the change
  type, then +1 level for a repo marked `priority: high` in repos.yml, and -1
  level for a repo with no commits in a year.

  These weights are judgement, not a validated model, and the README says so
  plainly. The point of centralising them is that the judgement is legible and
  changeable in one edit, rather than smeared across five models where nobody
  can tell what the ranking actually is.

  Levels are scored 1-4 so the adjustments are arithmetic, then mapped back to
  a label. Scores are clamped: nothing can be pushed above critical or below
  low, and in particular a high-priority repo being archived cannot overflow
  into an undefined fifth level.
#}

{% macro materiality_score(materiality_column) %}
    (case {{ materiality_column }}
        when 'critical' then 4
        when 'high'     then 3
        when 'medium'   then 2
        when 'low'      then 1
        else 1
     end)
{% endmacro %}


{% macro materiality_label(score_expression) %}
    (case
        when {{ score_expression }} >= 4 then 'critical'
        when {{ score_expression }} =  3 then 'high'
        when {{ score_expression }} =  2 then 'medium'
        else 'low'
     end)
{% endmacro %}


{#
  Days without a commit before a repo's changes get demoted. A year, per
  DESIGN.md. Deliberately much longer than the 180-day staleness flag used by
  int_activity_signals: 180 days makes a repo worth MENTIONING as stale, while
  a full year is what makes its other news worth less.
#}
{% macro abandoned_after_days() %}365{% endmacro %}

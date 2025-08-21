{% macro extract_year_from_title(title_col) %}
  TRY_TO_NUMBER(REGEXP_SUBSTR({{ title_col }}, '\\((\\d{4})\\)$', 1, 1, 'e', 1))
{% endmacro %}

{% macro strip_year_from_title(title_col) %}
  RTRIM(REGEXP_REPLACE({{ title_col }}, '\\s*\\(\\d{4}\\)$', ''))
{% endmacro %}
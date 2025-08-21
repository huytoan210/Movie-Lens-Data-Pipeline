{% macro epoch_to_ts(epoch_col) -%}
  TO_TIMESTAMP({{ epoch_col }})
{%- endmacro %}
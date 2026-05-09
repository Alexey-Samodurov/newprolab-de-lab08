{#
  Возвращает «логическую» дату прогона: либо переданную из Airflow через
  `--vars '{run_date: YYYY-MM-DD}'`, либо `current_date()` для ручных
  запусков dbt без оркестратора.

  Использовать как верхнюю границу окна инкремента вместо `current_date()`,
  чтобы catchup-run за прошлый день видел данные «на тот день», а не
  «на сегодня минус lookback».
#}
{% macro run_date() %}
  {%- set rd = var('run_date', '') | string | trim -%}
  {%- if rd -%}
    DATE '{{ rd }}'
  {%- else -%}
    current_date()
  {%- endif -%}
{% endmacro %}

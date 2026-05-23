{#
  Gold: дневной объём возвратов по дню отмены.
  Сумма берётся в нативной валюте — в bronze cancellations нет валюты
  исходной транзакции, поэтому в TGRK не конвертируем. Late-arriving
  отмены пересчитывают свой исторический cancel_day целиком.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='cancel_day',
    options={'primaryKey': 'cancel_day', 'preCombineField': 'updated_at', 'type': 'cow'}
) }}

WITH base AS (
    SELECT
        event_day        AS cancel_day,
        refund_amount,
        is_refund_invalid,
        ingested_at
    FROM {{ ref('cancellations_clean') }}
),
{% if is_incremental() %}
affected_days AS (
    SELECT DISTINCT cancel_day
    FROM base
    WHERE to_date(ingested_at) = {{ run_date() }}
),
src AS (
    SELECT b.*
    FROM base b
    JOIN affected_days a ON a.cancel_day = b.cancel_day
)
{% else %}
src AS (SELECT * FROM base)
{% endif %}
SELECT
    cancel_day,
    sum(coalesce(refund_amount, 0))                          AS refund_native_sum,
    count(*)                                                 AS refund_cnt,
    sum(CASE WHEN is_refund_invalid THEN 1 ELSE 0 END)       AS invalid_refund_cnt,
    max(ingested_at)                                         AS updated_at
FROM src
WHERE coalesce(refund_amount, 0) > 0
  AND is_refund_invalid = false
GROUP BY cancel_day

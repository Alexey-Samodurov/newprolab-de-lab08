{#
  Gold: Дневной объём refund-ов по дню отмены (`cancel_day`).

  ADR-003 (late-arriving): incremental + merge с пересчётом ЗАТРОНУТЫХ cancel_day.
  Late-arriving cancellation попадает в silver в день ingested_at, но
  обновляет event_day-партицию прошлого дня. В инкременте берём все
  cancellations_clean для тех cancel_day, по которым сегодня приехали
  новые строки (date(ingested_at)=run_date), пересчитываем агрегат
  целиком; MERGE по cancel_day заменяет старую строку.

  Refund сумма native — `refund_amount` из `cancellations_clean`. Конверсия
  в TGRK не делается на этом уровне: оригинальная валюта транзакции в bronze
  cancellations отсутствует.
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

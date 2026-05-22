{#
  Gold: Дневной объём refund-ов по дню отмены (`cancel_day`).

  ADR-003 (late-arriving): materialized=table — full rebuild на каждый run.
  Late-arriving cancellation попадает в silver в день ingested_at, но
  обновляет event_day-партицию прошлого дня. Инкрементный пересчёт только
  за run_date затёр бы корректный исторический агрегат частичными данными.
  Объём — одна строка на cancel_day, full rebuild дёшев.

  Refund сумма native — `refund_amount` из `cancellations_clean`. Конверсия
  в TGRK не делается на этом уровне: оригинальная валюта транзакции в bronze
  cancellations отсутствует.
#}
{{ config(materialized='table', unique_key='cancel_day', file_format='hudi',
   options={'primaryKey': 'cancel_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH src AS (
    SELECT
        event_day        AS cancel_day,
        refund_amount,
        is_refund_invalid,
        ingested_at
    FROM {{ ref('cancellations_clean') }}
)
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

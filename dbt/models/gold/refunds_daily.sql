{#
  Gold: Дневной объём refund-ов по дню отмены (`cancel_day`).

  ADR (FIX_PLAN P1-2): refund-ы относятся к дню ОТМЕНЫ, а не к дню исходной
  транзакции. Причина — daily-схема без пересчёта истории (см. P0-1):
  отмена за tx_day=N приходит в day=N+K и не должна задним числом править
  `revenue_daily[N]`. На дашборде показываем gross_revenue / refunds / refund_share
  как три параллельные витрины.

  Refund сумма native — `refund_amount` из `cancellations_clean`. Конверсия
  в TGRK не делается на этом уровне: оригинальная валюта транзакции в bronze
  cancellations отсутствует. На дашборде обычно достаточно nominal-сумм
  по кол-ву; для TGRK-эквивалента в будущем нужно протянуть `currency`
  из `transactions_clean` (см. P1-4 / future).
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge',
   unique_key='cancel_day',
   options={'primaryKey': 'cancel_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH src AS (
    SELECT
        event_day        AS cancel_day,
        refund_amount,
        is_refund_invalid,
        ingested_at
    FROM {{ ref('cancellations_clean') }}
    {% if is_incremental() %}
      WHERE event_day = {{ run_date() }}
    {% endif %}
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

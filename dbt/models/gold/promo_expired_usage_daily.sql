{#
  Gold: доля транзакций с просроченным промокодом — по дню транзакции.
  Считается per-transaction: промокод протух, если created_ts наступил
  строго после дня expiry_date. Метрика полезна для алертов на дыры
  в валидации промо на бэке.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge',
   unique_key='event_day',
   options={'primaryKey': 'event_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH src AS (
    SELECT * FROM {{ ref('transactions_clean') }}
    WHERE promo_code_id IS NOT NULL
      AND is_test_user = false
    {% if is_incremental() %}
      AND event_day = {{ run_date() }}
    {% endif %}
)
SELECT
    event_day,
    count(*)                                                        AS promo_uses_total,
    sum(CASE WHEN is_promo_expired_at_use THEN 1 ELSE 0 END)        AS promo_uses_expired,
    sum(CASE WHEN is_promo_expired_at_use AND status='completed'
             THEN 1 ELSE 0 END)                                     AS promo_uses_expired_completed,
    max(ingested_at)                                                AS updated_at
FROM src
GROUP BY event_day

{#
  Gold: Доля транзакций с просроченным промокодом — по дню транзакции.

  ADR (FIX_PLAN P1-3): метрика «промокод истёк на момент использования»
  считается на уровне отдельной транзакции (флаг `is_promo_expired_at_use`
  в transactions_clean), а не «когда-либо использовался после expiry»
  (см. `promo_codes_analysis.used_after_expiry` — это per-promo-code и over-history).

  Правило просрочки: created_ts >= expiry_date + 1 day (т.е. промокод
  действителен в течение дня expiry_date включительно).
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

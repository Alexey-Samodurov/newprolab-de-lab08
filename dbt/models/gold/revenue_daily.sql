{#
  Gold: Дневная выручка в базовой валюте TGRK.
  Конвертация: PUNK→TGRK = amount / rate_tgrk_punk; RUB→TGRK = amount / rate_tgrk_rub.
  Курс берётся за день транзакции (latest снимок в exchange_rates_daily).
  Если курса для дня нет — fill last известным.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge', unique_key='event_day',
   options={'primaryKey': 'event_day', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH tx AS (
    SELECT * FROM {{ ref('transactions_clean') }}
    WHERE is_revenue_eligible = true
      AND is_test_user = false
),
rates AS (
    SELECT * FROM {{ ref('exchange_rates_daily') }}
),
-- Forward-fill курсов: для каждого event_day берём max(rate_day) <= event_day
days_with_rate AS (
    SELECT t.event_day, max(r.rate_day) AS picked_rate_day
    FROM (SELECT DISTINCT event_day FROM tx) t
    LEFT JOIN rates r ON r.rate_day <= t.event_day
    GROUP BY t.event_day
),
day_rate AS (
    SELECT d.event_day, r.rate_tgrk_punk, r.rate_tgrk_rub
    FROM days_with_rate d
    LEFT JOIN rates r ON r.rate_day = d.picked_rate_day
),
tx_with_rate AS (
    SELECT tx.*, dr.rate_tgrk_punk, dr.rate_tgrk_rub
    FROM tx
    LEFT JOIN day_rate dr ON dr.event_day = tx.event_day
)
SELECT
    event_day,
    sum(CASE
          WHEN currency = 'TGRK' THEN amount
          WHEN currency = 'PUNK' AND rate_tgrk_punk IS NOT NULL AND rate_tgrk_punk <> 0
            THEN amount / rate_tgrk_punk
          WHEN currency = 'RUB'  AND rate_tgrk_rub  IS NOT NULL AND rate_tgrk_rub  <> 0
            THEN amount / rate_tgrk_rub
          ELSE NULL
        END) AS revenue_tgrk,
    count(*)                                          AS tx_cnt,
    sum(CASE WHEN currency='TGRK' THEN 1 ELSE 0 END)  AS tx_tgrk,
    sum(CASE WHEN currency='PUNK' THEN 1 ELSE 0 END)  AS tx_punk,
    sum(CASE WHEN currency='RUB'  THEN 1 ELSE 0 END)  AS tx_rub,
    current_timestamp()                                AS updated_at
FROM tx_with_rate
GROUP BY event_day

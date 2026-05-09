{#
  Silver: курсы валют в long-формате — одна строка на (rate_day, currency).

  ADR (FIX_PLAN P1-9): wide-схема bronze (`rate_tgrk_punk`, `rate_tgrk_rub`)
  плохо масштабируется на новые валюты — gold-модели вынуждены хардкодить
  список валют в `CASE WHEN currency = ...`. Long-схема позволяет
  `revenue_daily` джойнить транзакцию по `(event_day, currency)` без
  изменений при добавлении новой валюты.

  Семантика `rate_to_tgrk`: «сколько единиц <currency> за 1 TGRK».
    amount_tgrk = amount / rate_to_tgrk
  TGRK сам к себе = 1.0.

  Materialized=incremental merge: верхняя модель `exchange_rates_daily`
  пишет ровно одну строку на `run_date`, поэтому здесь добавляется/мерджится
  максимум 3 записи за прогон — по числу валют.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='rate_day_currency',
    options={
      'primaryKey': 'rate_day_currency',
      'preCombineField': 'timestamp',
      'type': 'cow'
    }
) }}

WITH d AS (
    SELECT * FROM {{ ref('exchange_rates_daily') }}
)
SELECT concat(rate_day, '|TGRK') AS rate_day_currency,
       rate_day, 'TGRK' AS currency, CAST(1.0 AS DOUBLE) AS rate_to_tgrk, timestamp
FROM d
UNION ALL
SELECT concat(rate_day, '|PUNK'), rate_day, 'PUNK', rate_tgrk_punk, timestamp
FROM d WHERE rate_tgrk_punk IS NOT NULL AND rate_tgrk_punk <> 0
UNION ALL
SELECT concat(rate_day, '|RUB'),  rate_day, 'RUB',  rate_tgrk_rub,  timestamp
FROM d WHERE rate_tgrk_rub  IS NOT NULL AND rate_tgrk_rub  <> 0

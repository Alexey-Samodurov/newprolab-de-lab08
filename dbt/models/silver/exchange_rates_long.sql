{#
  Silver: курсы валют в long-формате — одна строка на (rate_day, currency).
  Нужна, чтобы выручка джойнилась по валюте без хардкода списка валют:
  добавление новой валюты не требует менять gold-модели.
  rate_to_tgrk = «сколько единиц валюты за 1 TGRK», TGRK→TGRK = 1.
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

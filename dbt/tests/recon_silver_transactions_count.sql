-- DQ recon: количество строк в silver.transactions_clean должно равняться
-- количеству уникальных composite_pk в bronze.transactions (после dedup).
-- Сравниваем только данные внутри окна lookback, чтобы совпадать с инкрементальной
-- стратегией transactions_clean (исторические данные старше lookback не обрабатываются).
--
-- Anti-race с continuous bronze ingest: bronze пополняется long-running
-- streaming SparkApp'ом, а silver materialize-ится один раз за DAG-run.
-- Между materialize silver и запуском dbt test bronze может получить новые
-- строки в lookback-окне → bronze_unique будет на N больше silver_cnt без
-- реального DQ-issue. Поэтому bronze пиним по `ingested_at <= max(silver.ingested_at)`:
-- сравниваем только тот срез bronze, что был виден на момент materialize silver.
--
-- Тест возвращает 0 строк = пас.

WITH silver_max AS (
    SELECT max(ingested_at) AS max_ingested
    FROM {{ ref('transactions_clean') }}
    WHERE event_day >= date_sub({{ run_date() }}, {{ var('transactions_lookback_days', 30) }})
      AND event_day <= {{ run_date() }}
),
bronze_dedup AS (
    SELECT count(DISTINCT t.composite_pk) AS bronze_unique
    FROM {{ source('bronze', 'transactions') }} t
    CROSS JOIN silver_max s
    WHERE t.event_day >= date_sub({{ run_date() }}, {{ var('transactions_lookback_days', 30) }})
      AND t.event_day <= {{ run_date() }}
      AND t.ingested_at <= s.max_ingested
),
silver_cnt AS (
    SELECT count(*) AS silver_cnt
    FROM {{ ref('transactions_clean') }}
    WHERE event_day >= date_sub({{ run_date() }}, {{ var('transactions_lookback_days', 30) }})
      AND event_day <= {{ run_date() }}
)
SELECT bronze_unique, silver_cnt
FROM bronze_dedup, silver_cnt
WHERE bronze_unique <> silver_cnt

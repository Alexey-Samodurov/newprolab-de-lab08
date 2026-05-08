-- DQ recon: количество строк в silver.transactions_clean должно равняться
-- количеству уникальных composite_pk в bronze.transactions (после dedup).
-- Сравниваем только данные внутри окна lookback, чтобы совпадать с инкрементальной
-- стратегией transactions_clean (исторические данные старше lookback не обрабатываются).
-- Тест возвращает 0 строк = пас.

WITH bronze_dedup AS (
    SELECT count(DISTINCT composite_pk) AS bronze_unique
    FROM {{ source('bronze', 'transactions') }}
    WHERE event_day >= date_sub(current_date(), {{ var('transactions_lookback_days', 30) }})
),
silver_cnt AS (
    SELECT count(*) AS silver_cnt
    FROM {{ ref('transactions_clean') }}
    WHERE event_day >= date_sub(current_date(), {{ var('transactions_lookback_days', 30) }})
)
SELECT bronze_unique, silver_cnt
FROM bronze_dedup, silver_cnt
WHERE bronze_unique <> silver_cnt

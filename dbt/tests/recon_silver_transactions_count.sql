-- DQ recon: количество строк в silver.transactions_clean должно равняться
-- количеству уникальных composite_pk в bronze.transactions (после dedup).
-- Тест возвращает 0 строк = пас.

WITH bronze_dedup AS (
    SELECT count(DISTINCT composite_pk) AS bronze_unique
    FROM {{ source('bronze', 'transactions') }}
),
silver_cnt AS (
    SELECT count(*) AS silver_cnt
    FROM {{ ref('transactions_clean') }}
)
SELECT bronze_unique, silver_cnt
FROM bronze_dedup, silver_cnt
WHERE bronze_unique <> silver_cnt

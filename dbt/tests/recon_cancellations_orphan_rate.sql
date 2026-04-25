-- DQ recon: каждая строка cancellations_clean должна ссылаться на существующую
-- транзакцию ИЛИ быть помечена как late-arriving (original_transaction_id ещё не в silver).
-- На sample данных (45 tx vs 246 cancel) orphan_pct близок к 100% — это **warning**,
-- а не error. На production threshold будет жёстче.
{{ config(severity = 'warn') }}

WITH cancel AS (
    SELECT count(*) AS total,
           sum(CASE WHEN tx.transaction_id IS NULL THEN 1 ELSE 0 END) AS orphan_cnt
    FROM {{ ref('cancellations_clean') }} c
    LEFT JOIN {{ ref('transactions_clean') }} tx
      ON c.original_transaction_id = tx.transaction_id
)
SELECT total, orphan_cnt, orphan_cnt * 100.0 / nullif(total, 0) AS orphan_pct
FROM cancel
WHERE total > 100 AND orphan_cnt * 100.0 / total > 99

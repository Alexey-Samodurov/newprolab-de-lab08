{#
  Gold: Анализ отмен.
  Процент отмен от всех транзакций, разбивка по причинам, среднее время до отмены.
  Считается по дням, только по реальным пользователям.
  Джойним cancellations_clean → transactions_clean по original_transaction_id.
  Late-arriving cancellations обрабатываются корректно: дата берётся из отмены (event_day),
  а время до отмены = cancelled_ts - tx.created_ts.
#}
{{ config(materialized='incremental', file_format='hudi', incremental_strategy='merge',
   unique_key='pk',
   options={'primaryKey': 'pk', 'preCombineField': 'updated_at', 'type': 'cow'}) }}

WITH cancellations AS (
    SELECT
        c.cancellation_id,
        c.original_transaction_id,
        c.reason,
        c.cancelled_ts,
        c.refund_amount,
        c.event_day         AS cancel_day,
        c.is_refund_invalid
    FROM {{ ref('cancellations_clean') }} c
    {% if is_incremental() %}
      WHERE c.event_day >= date_sub(current_date(), {{ var('cancellations_lookback_days', 30) }})
    {% endif %}
),
-- Берём MIN(created_ts) per transaction_id чтобы избежать fan-out из-за
-- дублей transaction_id (~5% в silver). Джойним по transaction_id (не composite_pk),
-- т.к. именно это поле хранит cancellation.original_transaction_id.
tx_dedup AS (
    SELECT
        transaction_id,
        min(created_ts) AS created_ts,
        is_test_user
    FROM {{ ref('transactions_clean') }}
    WHERE is_test_user = false
    GROUP BY transaction_id, is_test_user
),
-- Линкуем отмену к транзакции (LEFT JOIN — отмена может быть без найденной транзакции)
joined AS (
    SELECT
        c.cancellation_id,
        c.original_transaction_id,
        c.reason,
        c.cancelled_ts,
        c.refund_amount,
        c.cancel_day,
        c.is_refund_invalid,
        t.created_ts        AS tx_created_ts,
        -- Время до отмены в секундах; NULL если транзакция не найдена
        CASE WHEN t.created_ts IS NOT NULL
             THEN (unix_timestamp(c.cancelled_ts) - unix_timestamp(t.created_ts))
             ELSE NULL END  AS seconds_to_cancel
    FROM cancellations c
    LEFT JOIN tx_dedup t ON t.transaction_id = c.original_transaction_id
)
SELECT
    concat(cancel_day, '_', coalesce(reason, 'unknown'))     AS pk,
    cancel_day,
    coalesce(reason, 'unknown')                              AS reason,
    count(*)                                                 AS cancellations_cnt,
    sum(CASE WHEN is_refund_invalid THEN 1 ELSE 0 END)       AS invalid_refund_cnt,
    sum(CASE WHEN tx_created_ts IS NULL THEN 1 ELSE 0 END)   AS orphan_cnt,
    avg(seconds_to_cancel)                                   AS avg_seconds_to_cancel,
    min(seconds_to_cancel)                                   AS min_seconds_to_cancel,
    max(seconds_to_cancel)                                   AS max_seconds_to_cancel,
    sum(coalesce(refund_amount, 0))                          AS total_refund_amount,
    current_timestamp()                                      AS updated_at
FROM joined
GROUP BY cancel_day, coalesce(reason, 'unknown')

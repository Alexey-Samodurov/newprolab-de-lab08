{#
  Gold: дневная сводка отмен в разрезе причины.
  Доля отмен от всех транзакций, количество и среднее время от покупки
  до отмены — по реальным пользователям. Late-arriving отмены корректно
  пересчитывают свой исторический день: при появлении новых строк
  агрегат за затронутые cancel_day строится заново.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='pk',
    options={'primaryKey': 'pk', 'preCombineField': 'updated_at', 'type': 'cow'}
) }}

WITH cancellations_base AS (
    SELECT
        c.cancellation_id,
        c.original_transaction_id,
        c.reason,
        c.cancelled_ts,
        c.refund_amount,
        c.event_day         AS cancel_day,
        c.is_refund_invalid,
        c.ingested_at
    FROM {{ ref('cancellations_clean') }} c
),
{% if is_incremental() %}
affected_days AS (
    SELECT DISTINCT cancel_day
    FROM cancellations_base
    WHERE to_date(ingested_at) = {{ run_date() }}
),
cancellations AS (
    SELECT c.*
    FROM cancellations_base c
    JOIN affected_days a ON a.cancel_day = c.cancel_day
),
{% else %}
cancellations AS (SELECT * FROM cancellations_base),
{% endif %}
tx_dedup AS (
    SELECT
        transaction_id,
        min(created_ts) AS created_ts,
        count(*)        AS tx_match_cnt,
        is_test_user
    FROM {{ ref('transactions_clean') }}
    WHERE is_test_user = false
    GROUP BY transaction_id, is_test_user
),
joined AS (
    SELECT
        c.cancellation_id,
        c.original_transaction_id,
        c.reason,
        c.cancelled_ts,
        c.refund_amount,
        c.cancel_day,
        c.is_refund_invalid,
        c.ingested_at,
        t.created_ts        AS tx_created_ts,
        coalesce(t.tx_match_cnt, 0) AS tx_match_cnt,
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
    sum(CASE WHEN tx_match_cnt > 1 THEN 1 ELSE 0 END)        AS ambiguous_attribution_cnt,
    avg(seconds_to_cancel)                                   AS avg_seconds_to_cancel,
    min(seconds_to_cancel)                                   AS min_seconds_to_cancel,
    max(seconds_to_cancel)                                   AS max_seconds_to_cancel,
    sum(coalesce(refund_amount, 0))                          AS total_refund_amount,
    max(ingested_at)                                         AS updated_at
FROM joined
GROUP BY cancel_day, coalesce(reason, 'unknown')

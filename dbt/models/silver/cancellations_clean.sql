{#
  Silver: Отмены, нормализованные.
  Линкуем на исходную транзакцию (flag для late-arriving).
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='cancellation_id',
    options={
      'primaryKey': 'cancellation_id',
      'preCombineField': 'ingested_at',
      'type': 'cow'
    },
    partition_by=['event_day']
) }}

WITH src AS (
    SELECT
        cancellation_id,
        original_transaction_id,
        reason,
        cancelled_ts,
        refund_amount,
        event_day,
        ingested_at,
        row_number() OVER (
            PARTITION BY cancellation_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'cancellations') }}
    {% if is_incremental() %}
      WHERE event_day >= date_sub(current_date(), {{ var('cancellations_lookback_days', 30) }})
    {% endif %}
)
SELECT
    cancellation_id,
    original_transaction_id,
    reason,
    cancelled_ts,
    refund_amount,
    event_day,
    ingested_at,
    CASE WHEN refund_amount IS NULL OR refund_amount < 0
         THEN true ELSE false END AS is_refund_invalid
FROM src
WHERE rn = 1

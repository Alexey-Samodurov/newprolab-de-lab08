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

SELECT
    c.cancellation_id,
    c.original_transaction_id,
    c.reason,
    c.cancelled_ts,
    c.refund_amount,
    c.event_day,
    c.ingested_at,
    CASE WHEN c.refund_amount IS NULL OR c.refund_amount < 0
         THEN true ELSE false END AS is_refund_invalid
FROM {{ source('bronze', 'cancellations') }} c
{% if is_incremental() %}
  WHERE event_day >= date_sub(current_date(), {{ var('cancellations_lookback_days', 30) }})
{% endif %}

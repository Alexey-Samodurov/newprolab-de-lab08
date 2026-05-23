{#
  Silver: нормализованные отмены транзакций.
  Линкуем на исходную покупку, помечаем late-arriving (отмена пришла
  позже самой транзакции). Ключ cancellation_pk = event_day|cancellation_id,
  потому что cancellation_id уникален только внутри дня.
  Инкремент по дню загрузки — отмена за прошлые дни корректно догоняет
  соответствующую event_day-партицию.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='cancellation_pk',
    options={
      'primaryKey': 'cancellation_pk',
      'preCombineField': 'ingested_at',
      'type': 'cow'
    },
    partition_by=['event_day']
) }}

WITH src AS (
    SELECT
        cancellation_pk,
        cancellation_id,
        original_transaction_id,
        reason,
        cancelled_ts,
        refund_amount,
        event_day,
        ingested_at,
        row_number() OVER (
            PARTITION BY cancellation_pk
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'cancellations') }}
    {% if is_incremental() %}
      WHERE to_date(ingested_at) = {{ run_date() }}
    {% endif %}
)
SELECT
    cancellation_pk,
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

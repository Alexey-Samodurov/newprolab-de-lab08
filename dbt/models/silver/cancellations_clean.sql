{#
  Silver: Отмены, нормализованные.
  Линкуем на исходную транзакцию (flag для late-arriving).

  Hudi index: GLOBAL_BLOOM + update.partition.path=true.
  Причина — partition_by=event_day (производное от cancelled_ts), а unique_key
  cancellation_id с partition path не связан. Без global-index одинаковый
  cancellation_id из разных микро-батчей с разным cancelled_ts уезжает в
  разные партиции и BLOOM (partition-scoped) их не дедупит → cross-partition
  дубли в silver. См. spark/utils/hudi.py: hudi_opts(global_index=...).

  ADR-003 (late-arriving): инкремент по дню ЗАГРУЗКИ (date(ingested_at) = run_date),
  а не по event_day. Late-arriving cancellation за event_day=ds−k попадает в
  свой обычный daily run по дню ingested_at; Hudi GLOBAL_BLOOM перепишет
  правильную event_day-партицию. Downstream gold cancellations_summary /
  refunds_daily пересчитываются как materialized=table.
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='cancellation_id',
    options={
      'primaryKey': 'cancellation_id',
      'preCombineField': 'ingested_at',
      'type': 'cow',
      'hoodie.index.type': 'GLOBAL_BLOOM',
      'hoodie.bloom.index.update.partition.path': 'true'
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
      WHERE to_date(ingested_at) = {{ run_date() }}
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

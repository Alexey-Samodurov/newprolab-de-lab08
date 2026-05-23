{#
  Silver: Отмены, нормализованные.
  Линкуем на исходную транзакцию (flag для late-arriving).

  Ключ — `cancellation_pk = event_day|cancellation_id`. Сам
  `cancellation_id` рестартует с 1 в каждом дневном файле S3, поэтому
  он не уникален между днями и не годится как primary key. Композитный
  pk полностью лежит внутри одной event_day-партиции — GLOBAL_BLOOM
  не нужен, обычный BLOOM корректно дедупит.

  ADR-003 (late-arriving): инкремент по дню ЗАГРУЗКИ (date(ingested_at) = run_date),
  а не по event_day. Late-arriving cancellation за event_day=ds−k попадает в
  обычный daily run по дню ingested_at; так как `cancelled_ts` (и значит
  event_day и cancellation_pk) консистентен между ре-доставками, Hudi
  корректно обновит соответствующую event_day-партицию. Downstream gold
  cancellations_summary / refunds_daily пересчитываются как incremental
  с merge по cancel_day.

  `ingested_at` в bronze — это logical date Airflow-рана
  (`to_timestamp(ds)`), не wall-clock. Так backfill детерминирован, а
  фильтр `to_date(ingested_at) = run_date()` совпадает с тем же `ds`,
  которым обрабатывался бронзовый батч.
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

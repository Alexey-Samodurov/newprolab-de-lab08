{#
  Silver: Чистые транзакции.
  Дедуп по composite_pk (берём latest по ingested_at), приведение типов и enrichment.
  ADR: транзакции с пустым/несуществующим user_id оставляем (помечаем флагом),
       чтобы бизнес мог посчитать обе стороны (с/без unmatched).
#}
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='composite_pk',
    options={
      'primaryKey': 'composite_pk',
      'preCombineField': 'ingested_at',
      'type': 'cow'
    },
    partition_by=['event_day']
) }}

WITH src AS (
    SELECT
        composite_pk,
        transaction_id,
        user_id,
        user_uuid,
        amount,
        currency,
        transaction_type,
        promo_code_id,
        status,
        created_at,
        to_timestamp(from_unixtime(created_at)) AS created_ts,
        event_day,
        ingested_at,
        row_number() OVER (
            PARTITION BY composite_pk
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
      WHERE event_day = {{ run_date() }}
    {% endif %}
),
dedup AS (
    SELECT * FROM src WHERE rn = 1
),
tx_id_dup AS (
    SELECT transaction_id
    FROM dedup
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING count(*) > 1
),
flagged AS (
    SELECT
        d.composite_pk,
        d.transaction_id,
        d.user_id,
        d.user_uuid,
        d.amount,
        d.currency,
        d.transaction_type,
        d.promo_code_id,
        d.status,
        d.created_at,
        d.created_ts,
        d.event_day,
        d.ingested_at,
        CASE WHEN d.user_id IS NULL THEN true ELSE false END        AS is_user_missing,
        CASE WHEN u.user_id IS NULL AND d.user_id IS NOT NULL
             THEN true ELSE false END                               AS is_user_unknown,
        CASE WHEN tu.test_user_uuid IS NOT NULL OR coalesce(u.is_test_user, false)
             THEN true ELSE false END                               AS is_test_user,
        CASE WHEN d.user_id IS NOT NULL
                  AND coalesce(u.is_test_user, false) <> (tu.test_user_uuid IS NOT NULL)
             THEN true ELSE false END                               AS is_test_user_inconsistent,
        CASE WHEN d.amount IS NULL OR d.amount <= 0
             THEN true ELSE false END                               AS is_amount_invalid,
        CASE WHEN d.status = 'completed' AND d.transaction_type = 'purchase'
             THEN true ELSE false END                               AS is_revenue_eligible,
        CASE WHEN d.transaction_id IS NOT NULL AND dup.transaction_id IS NOT NULL
             THEN true ELSE false END                               AS is_transaction_id_duplicated,
        CASE WHEN d.promo_code_id IS NOT NULL
                  AND pc.expiry_date IS NOT NULL
                  AND to_date(from_unixtime(d.created_at)) >= date_add(pc.expiry_date, 1)
             THEN true ELSE false END                               AS is_promo_expired_at_use
    FROM dedup d
    LEFT JOIN {{ source('bronze', 'users') }}      u   ON d.user_id = u.user_id
    LEFT JOIN {{ source('bronze', 'test_users') }} tu  ON d.user_uuid = tu.test_user_uuid
    LEFT JOIN {{ source('bronze', 'promo_codes') }} pc ON d.promo_code_id = pc.promo_code_id
    LEFT JOIN tx_id_dup dup                            ON d.transaction_id = dup.transaction_id
)
SELECT * FROM flagged

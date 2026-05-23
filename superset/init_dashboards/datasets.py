"""Dataset upsert helpers for the gold-layer Hudi tables."""

from __future__ import annotations

import time

from ._log import log
from .client import SupersetClient


GOLD_SCHEMA = "gold"

DATASET_MAIN_DTTM: dict[str, str] = {
    "transactions_by_hour_unified": "event_day",
    "purchases_by_hour_unified": "event_day",
    "revenue_daily": "event_day",
    "refunds_daily": "cancel_day",
    "cancellations_summary_unified": "cancel_day",
    "user_cohorts": "event_day",
    "promo_expired_usage_daily": "event_day",
    "dq_summary_daily": "event_day",
}

DATASET_SQL: dict[str, str] = {
    "transactions_by_hour_unified": """
SELECT pk, event_day, hour_of_day, is_test_user,
       tx_cnt, completed_cnt, failed_cnt,
       CAST(updated_at AS TIMESTAMP(6)) AS updated_at,
       'settled' AS source
FROM gold.transactions_by_hour
UNION ALL
SELECT l.pk, l.event_day, l.hour_of_day, l.is_test_user,
       l.tx_cnt, l.completed_cnt, l.failed_cnt,
       from_unixtime(l.updated_at / 1000000) AS updated_at,
       'live' AS source
FROM gold.transactions_by_hour_live l
WHERE l.event_day > COALESCE(
    (SELECT max(event_day) FROM gold.transactions_by_hour),
    '1970-01-01'
)
""".strip(),
    "purchases_by_hour_unified": """
SELECT pk, event_day, hour_of_day,
       purchase_cnt, gross_amount_native,
       CAST(updated_at AS TIMESTAMP(6)) AS updated_at,
       'settled' AS source
FROM gold.purchases_by_hour
UNION ALL
SELECT l.pk, l.event_day, l.hour_of_day,
       l.purchase_cnt, l.gross_amount_native,
       from_unixtime(l.updated_at / 1000000) AS updated_at,
       'live' AS source
FROM gold.purchases_by_hour_live l
WHERE l.event_day > COALESCE(
    (SELECT max(event_day) FROM gold.purchases_by_hour),
    '1970-01-01'
)
""".strip(),
    "cancellations_summary_unified": """
SELECT pk, cancel_day, reason,
       cancellations_cnt, invalid_refund_cnt, orphan_cnt,
       ambiguous_attribution_cnt, avg_seconds_to_cancel,
       min_seconds_to_cancel, max_seconds_to_cancel,
       total_refund_amount,
       CAST(updated_at AS TIMESTAMP(6)) AS updated_at,
       'settled' AS source
FROM gold.cancellations_summary
UNION ALL
SELECT l.pk, l.cancel_day, l.reason,
       l.cancellations_cnt, l.invalid_refund_cnt, l.orphan_cnt,
       l.ambiguous_attribution_cnt, l.avg_seconds_to_cancel,
       l.min_seconds_to_cancel, l.max_seconds_to_cancel,
       l.total_refund_amount,
       from_unixtime(l.updated_at / 1000000) AS updated_at,
       'live' AS source
FROM gold.cancellations_summary_live l
WHERE l.cancel_day > COALESCE(
    (SELECT max(cancel_day) FROM gold.cancellations_summary),
    '1970-01-01'
)
""".strip(),
}


def upsert_dataset(client: SupersetClient, db_id: int, table: str) -> int | None:
    """Create the dataset if missing and set ``main_dttm_col`` when applicable.

    For tables listed in :data:`DATASET_SQL` a virtual dataset is created
    (Superset stores the SQL and ``table_name`` as a logical alias). For
    everything else a physical dataset bound to the Trino table is created.

    Args:
        client: Authenticated Superset client.
        db_id: Target database id.
        table: Gold table or virtual-dataset alias.

    Returns:
        Dataset id, or ``None`` if the underlying Trino table does not exist yet.
    """
    sql = DATASET_SQL.get(table)
    existing = client.find_id("/api/v1/dataset/", "table_name", table)
    if existing:
        ds_id = existing
        log.info("Dataset '%s.%s' exists (id=%s).", GOLD_SCHEMA, table, ds_id)
    else:
        payload: dict = {"database": db_id, "table_name": table}
        if sql:
            # Virtual dataset: schema выводится Superset-ом из SQL, передавать
            # его не нужно (иначе на части версий ловим 500 Fatal error).
            payload["sql"] = sql
        else:
            payload["schema"] = GOLD_SCHEMA
        r = client.session.post(f"{client.host}/api/v1/dataset/", json=payload)
        if r.status_code == 422 and "could not be found" in r.text:
            log.warning("Dataset '%s.%s': таблица ещё не создана в Trino — skip.", GOLD_SCHEMA, table)
            return None
        if not r.ok:
            log.error("POST /api/v1/dataset/ failed (%s) for table=%s payload_keys=%s body=%s",
                      r.status_code, table, list(payload.keys()), r.text[:800])
            r.raise_for_status()
        ds_id = r.json()["id"]
        log.info("Created %sdataset '%s.%s' id=%s", "virtual " if sql else "", GOLD_SCHEMA, table, ds_id)
        time.sleep(0.5)

    dttm = DATASET_MAIN_DTTM.get(table)
    if dttm:
        cols = client.get(f"/api/v1/dataset/{ds_id}").get("result", {}).get("columns", [])
        col_names = {c["column_name"] for c in cols}
        if dttm in col_names:
            updated_cols = []
            for c in cols:
                col_payload = {
                    "id": c["id"],
                    "column_name": c["column_name"],
                    "type": c.get("type"),
                    "is_dttm": True if c["column_name"] == dttm else bool(c.get("is_dttm")),
                    "filterable": bool(c.get("filterable", True)),
                    "groupby": bool(c.get("groupby", True)),
                    "verbose_name": c.get("verbose_name"),
                    "description": c.get("description"),
                    "expression": c.get("expression"),
                    "extra": c.get("extra"),
                    "python_date_format": c.get("python_date_format"),
                    "uuid": c.get("uuid"),
                }
                updated_cols.append({k: v for k, v in col_payload.items() if v is not None or k == "is_dttm"})
            client.put(f"/api/v1/dataset/{ds_id}", {
                "main_dttm_col": dttm,
                "columns": updated_cols,
            })
            log.info("main_dttm_col=%s", dttm)
        else:
            log.warning("колонка '%s' не найдена в датасете %s, time-series viz не сработает", dttm, table)
    return ds_id

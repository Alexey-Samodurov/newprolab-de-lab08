"""Dataset upsert helpers for the gold-layer Hudi tables."""

from __future__ import annotations

import time

from ._log import log
from .client import SupersetClient


GOLD_SCHEMA = "gold"

DATASET_MAIN_DTTM: dict[str, str] = {
    "transactions_by_hour": "event_day",
    "purchases_by_hour": "event_day",
    "revenue_daily": "event_day",
    "refunds_daily": "cancel_day",
    "cancellations_summary": "cancel_day",
    "user_cohorts": "event_day",
    "promo_expired_usage_daily": "event_day",
    "dq_summary_daily": "event_day",
}


def upsert_dataset(client: SupersetClient, db_id: int, table: str) -> int | None:
    """Create the dataset if missing and set ``main_dttm_col`` when applicable.

    Args:
        client: Authenticated Superset client.
        db_id: Target database id.
        table: Gold table name.

    Returns:
        Dataset id, or ``None`` if the Trino table does not exist yet.
    """
    existing = client.find_id("/api/v1/dataset/", "table_name", table)
    if existing:
        ds_id = existing
        log.info("Dataset '%s.%s' exists (id=%s).", GOLD_SCHEMA, table, ds_id)
    else:
        r = client.session.post(
            f"{client.host}/api/v1/dataset/",
            json={"database": db_id, "schema": GOLD_SCHEMA, "table_name": table},
        )
        if r.status_code == 422 and "could not be found" in r.text:
            log.warning("Dataset '%s.%s': таблица ещё не создана в Trino — skip.", GOLD_SCHEMA, table)
            return None
        if not r.ok:
            log.error("%s: %s", r.status_code, r.text[:300])
            r.raise_for_status()
        ds_id = r.json()["id"]
        log.info("Created dataset '%s.%s' id=%s", GOLD_SCHEMA, table, ds_id)
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

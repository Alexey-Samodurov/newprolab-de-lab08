"""Superset bootstrap for the Lab08 transaction analytics dashboard.

The package is invoked via ``python -m init_dashboards`` and is idempotent:
entities are looked up by name and updated (PUT) instead of recreated, so
manual UI tweaks and chart ids stay stable across runs.
"""

from __future__ import annotations

import argparse

from ._log import log
from .charts import build_charts, cleanup_obsolete_charts
from .client import SupersetClient
from .dashboard import upsert_dashboard
from .datasets import upsert_dataset
from .trino import upsert_database


DASHBOARD_TITLE = "Lab08 — Transaction Analytics"

GOLD_TABLES: tuple[str, ...] = (
    "transactions_by_hour",
    "purchases_by_hour",
    "revenue_daily",
    "refunds_daily",
    "promo_codes_analysis",
    "promo_expired_usage_daily",
    "cancellations_summary",
    "user_cohorts",
    "dq_summary_daily",
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for Superset host, username and password.

    Returns:
        Parsed arguments with ``host``, ``user`` and ``password`` fields.
    """
    parser = argparse.ArgumentParser(description="Superset bootstrap for Lab08")
    parser.add_argument("--host", default="http://localhost:8088")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="admin")
    return parser.parse_args()


def main() -> None:
    """Run the four-phase Superset bootstrap (DB, datasets, charts, dashboard)."""
    args = parse_args()
    log.info("Connecting to Superset at %s as %s...", args.host, args.user)
    client = SupersetClient(args.host, args.user, args.password)

    log.info("[1/4] Database connection...")
    db_id = upsert_database(client)

    log.info("[2/4] Datasets...")
    ds = {t: upsert_dataset(client, db_id, t) for t in GOLD_TABLES}

    log.info("[3/4] Charts...")
    cleanup_obsolete_charts(client)
    charts = build_charts(client, ds)

    log.info("[4/4] Dashboard...")
    if not any(charts.values()):
        log.warning("Нет ни одного чарта (gold таблицы пока пусты). Запусти DAG transactions_pipeline и повтори.")
        return
    upsert_dashboard(client, DASHBOARD_TITLE, charts, ds)

    log.info("Done! Open Superset → Dashboards → '%s'.", DASHBOARD_TITLE)


__all__ = ["main", "parse_args", "DASHBOARD_TITLE", "GOLD_TABLES"]

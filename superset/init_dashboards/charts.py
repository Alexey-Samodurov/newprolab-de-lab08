"""Chart upsert helpers and the full Lab08 dashboard chart catalogue."""

from __future__ import annotations

import json
from typing import Any

from ._log import log
from .client import SupersetClient


def simple_metric(column: str, agg: str, label: str) -> dict:
    """Build a SIMPLE Superset metric definition.

    Args:
        column: Source column name.
        agg: Aggregate function (e.g. ``"SUM"``, ``"AVG"``).
        label: Display label.

    Returns:
        Metric definition dict.
    """
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": agg,
        "label": label,
    }


def sql_metric(sql: str, label: str) -> dict:
    """Build a SQL Superset metric definition.

    Args:
        sql: SQL expression evaluated by the backend.
        label: Custom display label.

    Returns:
        Metric definition dict.
    """
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label,
        "hasCustomLabel": True,
    }


def adhoc_filter(col: str, op: str, val: Any) -> dict:
    """Build a SIMPLE ad-hoc WHERE filter clause.

    Args:
        col: Column name.
        op: Comparison operator (e.g. ``"=="``, ``"IN"``).
        val: Right-hand comparator value.

    Returns:
        Filter clause dict.
    """
    return {
        "expressionType": "SIMPLE",
        "subject": col,
        "operator": op,
        "comparator": val,
        "clause": "WHERE",
    }


REAL_USERS_FILTER = adhoc_filter("is_test_user", "==", False)


def upsert_chart(client: SupersetClient, payload: dict) -> int | None:
    """Idempotent chart upsert: PUT by id when present, otherwise POST.

    Args:
        client: Authenticated Superset client.
        payload: Chart payload (must include ``slice_name``).

    Returns:
        Chart id, or ``None`` when the underlying dataset is missing.
    """
    if payload.get("datasource_id") is None:
        log.warning("Chart '%s': skip (нет датасета)", payload['slice_name'])
        return None
    existing = client.find_id("/api/v1/chart/", "slice_name", payload["slice_name"])
    if existing:
        update_payload = {
            k: v for k, v in payload.items()
            if k in {"slice_name", "description", "viz_type", "params"}
        }
        client.put(f"/api/v1/chart/{existing}", update_payload)
        log.info("Updated chart '%s' id=%s", payload['slice_name'], existing)
        return existing
    chart_id = client.post("/api/v1/chart/", payload)["id"]
    log.info("Created chart '%s' id=%s", payload['slice_name'], chart_id)
    return chart_id


def cleanup_obsolete_charts(client: SupersetClient) -> None:
    """Delete charts that belonged to older versions of this script.

    Args:
        client: Authenticated Superset client.
    """
    for stale in (
        "Daily Revenue in TGRK",
        "Promo Codes: Over-Limit Uses",
        "Purchases by Hour (Share)",
        "KPI: Completed %",
        "Daily Revenue (TGRK)",
        "Daily Transactions by Status",
        "Transactions by Hour",
        "Purchases by Hour",
        "Cancellations by Reason (daily)",
        "User Cohorts: New vs Returning",
        "Promo Codes Analysis",
    ):
        sid = client.find_id("/api/v1/chart/", "slice_name", stale)
        if sid:
            client.delete(f"/api/v1/chart/{sid}")
            log.info("Removed obsolete chart '%s' (id=%s)", stale, sid)


def build_charts(client: SupersetClient, ds: dict[str, int | None]) -> dict[str, int | None]:
    """Create or update all dashboard charts.

    Args:
        client: Authenticated Superset client.
        ds: Mapping of gold table name to dataset id.

    Returns:
        Mapping ``slice_name`` -> ``chart_id`` (id may be ``None``).
    """
    charts: dict[str, int | None] = {}

    charts["KPI: Total Revenue (TGRK)"] = upsert_chart(client, {
        "slice_name": "KPI: Total Revenue (TGRK)",
        "description": "Совокупная GROSS-выручка в TGRK по реальным пользователям (без вычета "
                       "refunds — см. отдельный график Daily Refunds). Конвертация PUNK/RUB → "
                       "TGRK через дневные курсы (forward+backward fill из exchange_rates_long).",
        "viz_type": "big_number_total",
        "datasource_id": ds["revenue_daily"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": simple_metric("gross_revenue_tgrk", "SUM", "Gross Revenue, TGRK"),
            "y_axis_format": "~s",
            "subheader": "сумма за весь период (gross)",
        }),
    })

    charts["KPI: Total Transactions"] = upsert_chart(client, {
        "slice_name": "KPI: Total Transactions",
        "description": "Общее число транзакций реальных пользователей (любой статус).",
        "viz_type": "big_number_total",
        "datasource_id": ds["transactions_by_hour"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": simple_metric("tx_cnt", "SUM", "Transactions"),
            "adhoc_filters": [REAL_USERS_FILTER],
            "y_axis_format": "SMART_NUMBER",
            "subheader": "транзакций реальных юзеров (любой статус)",
        }),
    })

    charts["KPI: Invalid Refund %"] = upsert_chart(client, {
        "slice_name": "KPI: Invalid Refund %",
        "description": "Доля отмен с поломанной refund-семантикой (is_refund_invalid=true: "
                       "refund_amount > суммы транзакции, или refund для не-purchase). "
                       "DQ-индикатор: высокое значение → проблема в источнике или маппинге.",
        "viz_type": "big_number_total",
        "datasource_id": ds["cancellations_summary"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": sql_metric(
                "ROUND(SUM(invalid_refund_cnt) * 100.0 / NULLIF(SUM(cancellations_cnt), 0), 1)",
                "Invalid Refund %",
            ),
            "number_format": ".1f",
            "y_axis_format": ".1f",
            "subheader": "% отмен с поломанным refund (DQ)",
        }),
    })

    charts["KPI: Cancellations"] = upsert_chart(client, {
        "slice_name": "KPI: Cancellations",
        "description": "Общее число отмен (по всем причинам, реальные пользователи).",
        "viz_type": "big_number_total",
        "datasource_id": ds["cancellations_summary"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": simple_metric("cancellations_cnt", "SUM", "Cancellations"),
            "y_axis_format": "SMART_NUMBER",
            "subheader": "всего отмен за период",
        }),
    })

    charts["KPI: User-days (DAU sum)"] = upsert_chart(client, {
        "slice_name": "KPI: User-days (DAU sum)",
        "description": "Сумма дневных уникальных пользователей (DAU) за период. "
                       "Это user-days, а не уникальные пользователи за период — один человек, "
                       "активный N дней, считается N раз. Включает new + returning.",
        "viz_type": "big_number_total",
        "datasource_id": ds["user_cohorts"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": simple_metric("unique_users", "SUM", "User-days"),
            "y_axis_format": "SMART_NUMBER",
            "subheader": "сумма DAU за период (user-days)",
        }),
    })

    charts["Daily Revenue (TGRK)"] = upsert_chart(client, {
        "slice_name": "Daily Revenue (TGRK) — выручка по дням, TGRK",
        "description": "Дневная GROSS-выручка в TGRK (без вычета refunds). Конвертация валют "
                       "по дневным курсам (forward+backward fill из exchange_rates_long).",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": ds["revenue_daily"],
        "datasource_type": "table",
        "params": json.dumps({
            "x_axis": "event_day",
            "time_grain_sqla": "P1D",
            "metrics": [simple_metric("gross_revenue_tgrk", "SUM", "Gross Revenue, TGRK")],
            "groupby": [],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "y_axis_format": "~s",
            "x_axis_title": "Day",
            "y_axis_title": "Gross Revenue, TGRK",
            "y_axis_title_margin": 40,
            "x_axis_title_margin": 30,
            "xAxisLabelRotation": -45,
            "truncateYAxis": True,
            "show_legend": False,
            "rich_tooltip": True,
            "markerEnabled": True,
        }),
    })

    charts["Daily Transactions by Status"] = upsert_chart(client, {
        "slice_name": "Daily Transactions by Status — транзакции по дням и статусам",
        "description": "Дневной объём транзакций реальных пользователей: completed vs failed vs other.",
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": ds["transactions_by_hour"],
        "datasource_type": "table",
        "params": json.dumps({
            "x_axis": "event_day",
            "time_grain_sqla": "P1D",
            "metrics": [
                simple_metric("completed_cnt", "SUM", "Completed"),
                simple_metric("failed_cnt", "SUM", "Failed"),
                sql_metric("SUM(tx_cnt) - SUM(completed_cnt) - SUM(failed_cnt)", "Other"),
            ],
            "adhoc_filters": [REAL_USERS_FILTER],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "stack": "Stack",
            "y_axis_format": "~s",
            "x_axis_title": "Day",
            "y_axis_title": "Transactions",
            "y_axis_title_margin": 40,
            "x_axis_title_margin": 30,
            "xAxisLabelRotation": -45,
            "truncateYAxis": True,
            "show_legend": True,
            "rich_tooltip": True,
        }),
    })

    charts["Transactions by Hour"] = upsert_chart(client, {
        "slice_name": "Transactions by Hour — нагрузка по часам (test vs real)",
        "description": "Распределение транзакций по часам суток с разбивкой test vs real пользователи. "
                       "Тестовые ~20% в будни — отлично видно паттерн.",
        "viz_type": "dist_bar",
        "datasource_id": ds["transactions_by_hour"],
        "datasource_type": "table",
        "params": json.dumps({
            "metrics": [simple_metric("tx_cnt", "SUM", "Transactions")],
            "groupby": ["hour_of_day"],
            "columns": ["is_test_user"],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "y_axis_format": "~s",
            "x_axis_label": "Hour of day",
            "y_axis_label": "Transactions",
            "left_margin": 80,
            "bottom_margin": 60,
            "x_ticks_layout": "45°",
        }),
    })

    charts["Purchases by Hour"] = upsert_chart(client, {
        "slice_name": "Purchases by Hour — успешные покупки по часам",
        "description": "Только успешные покупки (status=completed AND transaction_type=purchase) "
                       "по часам суток, реальные пользователи.",
        "viz_type": "dist_bar",
        "datasource_id": ds["purchases_by_hour"],
        "datasource_type": "table",
        "params": json.dumps({
            "metrics": [simple_metric("purchase_cnt", "SUM", "Purchases")],
            "groupby": ["hour_of_day"],
            "row_limit": 24,
            "color_scheme": "supersetColors",
            "show_legend": False,
            "y_axis_format": "~s",
            "x_axis_label": "Hour of day",
            "y_axis_label": "Purchases",
            "left_margin": 80,
            "bottom_margin": 60,
            "x_ticks_layout": "45°",
        }),
    })

    charts["Cancellations by Reason (daily)"] = upsert_chart(client, {
        "slice_name": "Cancellations by Reason — отмены по дням и причинам",
        "description": "Динамика отмен по дням, разбивка по причинам (stacked).",
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": ds["cancellations_summary"],
        "datasource_type": "table",
        "params": json.dumps({
            "x_axis": "cancel_day",
            "time_grain_sqla": "P1D",
            "metrics": [simple_metric("cancellations_cnt", "SUM", "Cancellations")],
            "groupby": ["reason"],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "stack": "Stack",
            "show_legend": True,
            "y_axis_format": "~s",
            "x_axis_title": "Day",
            "y_axis_title": "Cancellations",
            "y_axis_title_margin": 40,
            "x_axis_title_margin": 30,
            "xAxisLabelRotation": -45,
            "truncateYAxis": True,
            "rich_tooltip": True,
        }),
    })

    charts["User Cohorts: New vs Returning"] = upsert_chart(client, {
        "slice_name": "User Cohorts — новые vs вернувшиеся по дням",
        "description": "Уникальные пользователи по дням, разбивка по когорте: "
                       "new (первая транзакция в этот день) vs returning.",
        "viz_type": "echarts_area",
        "datasource_id": ds["user_cohorts"],
        "datasource_type": "table",
        "params": json.dumps({
            "x_axis": "event_day",
            "time_grain_sqla": "P1D",
            "metrics": [simple_metric("unique_users", "SUM", "Unique users")],
            "groupby": ["user_type"],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "stack": "Stack",
            "show_legend": True,
            "y_axis_format": "~s",
            "x_axis_title": "Day",
            "y_axis_title": "Unique users",
            "y_axis_title_margin": 40,
            "x_axis_title_margin": 30,
            "xAxisLabelRotation": -45,
            "truncateYAxis": True,
            "rich_tooltip": True,
            "opacity": 0.5,
        }),
    })

    charts["Promo Codes Analysis"] = upsert_chart(client, {
        "slice_name": "Promo Codes — использование, лимиты, аномалии",
        "description": "Использования промокодов: всего / completed / failed, лимит, флаги "
                       "over_limit и used_after_expiry. Сортировка по uses_total ↓.",
        "viz_type": "table",
        "datasource_id": ds["promo_codes_analysis"],
        "datasource_type": "table",
        "params": json.dumps({
            "query_mode": "raw",
            "all_columns": [
                "code", "uses_total", "uses_completed", "uses_failed",
                "max_uses", "over_limit", "used_after_expiry",
                "expiry_date", "first_used_at", "last_used_at",
            ],
            "order_by_cols": ['["uses_total", false]'],
            "row_limit": 1000,
            "table_timestamp_format": "smart_date",
            "show_cell_bars": True,
            "color_pn": True,
            "include_search": True,
            "page_length": 25,
        }),
    })

    return charts

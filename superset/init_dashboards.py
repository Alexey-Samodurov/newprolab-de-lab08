#!/usr/bin/env python3
"""
Superset bootstrap: создаёт database connection, datasets, charts и дашборд
"Lab08 — Transaction Analytics" через REST API.

Использование:
    python superset/init_dashboards.py [--host http://localhost:8088] [--user admin] [--password admin]

Что создаётся:
  1. Database connection → Trino (lab08-trino)
  2. Datasets (silver_gold.*) с проставленным main_dttm_col для time-series
  3. Charts:
     KPI-полоса (Big Number):
       - KPI: Total Revenue (TGRK)
       - KPI: Total Transactions
       - KPI: Completed %
       - KPI: Cancellations
       - KPI: Active Users
     Тренды:
       - Daily Revenue (TGRK) — line
       - Daily Transactions by Status — stacked bar
     Операционные разрезы:
       - Transactions by Hour (test vs real) — grouped bar
       - Purchases by Hour — bar
       - Cancellations by Reason — stacked bar по дням
     Пользователи и промокоды:
       - User Cohorts: New vs Returning — stacked area
       - Promo Codes Analysis — table
  4. Dashboard "Lab08 — Transaction Analytics" с tabs (Overview / Operations / Users & Promo)
     и нативными фильтрами (event_day, is_test_user).

Идемпотентно: сущности обновляются по имени (PUT), а не пересоздаются, чтобы
ручные правки в UI и chart_id оставались стабильными.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any

import requests


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Superset bootstrap for Lab08")
    p.add_argument("--host", default="http://localhost:8088")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="admin")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Superset HTTP client
# ---------------------------------------------------------------------------

class SupersetClient:
    def __init__(self, host: str, user: str, password: str) -> None:
        self.host = host.rstrip("/")
        self.session = requests.Session()
        self._login(user, password)

    def _login(self, user: str, password: str) -> None:
        resp = self.session.post(
            f"{self.host}/api/v1/security/login",
            json={"username": user, "password": password, "provider": "db", "refresh": True},
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        csrf = self.session.get(f"{self.host}/api/v1/security/csrf_token/")
        csrf.raise_for_status()
        self.session.headers.update({"X-CSRFToken": csrf.json()["result"]})

    def _check(self, r: requests.Response) -> requests.Response:
        if not r.ok:
            print(f"  ERROR {r.status_code} {r.request.method} {r.url}: {r.text[:400]}", file=sys.stderr)
            r.raise_for_status()
        return r

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """HTTP с автоматическим retry на 429 (Superset rate limiter)."""
        url = f"{self.host}{path}"
        backoff = 1.0
        for attempt in range(6):
            r = self.session.request(method, url, **kwargs)
            if r.status_code != 429:
                return r
            wait = float(r.headers.get("Retry-After", backoff))
            print(f"  429 rate-limited on {method} {path}; sleep {wait:.1f}s (attempt {attempt + 1}/6)")
            time.sleep(wait)
            backoff = min(backoff * 2, 16.0)
        return r  # последняя попытка — отдадим как есть для _check

    def get(self, path: str, **kwargs: Any) -> dict:
        return self._check(self._request("GET", path, **kwargs)).json()

    def post(self, path: str, payload: dict) -> dict:
        return self._check(self._request("POST", path, json=payload)).json()

    def put(self, path: str, payload: dict) -> dict:
        return self._check(self._request("PUT", path, json=payload)).json()

    def delete(self, path: str) -> None:
        self._check(self._request("DELETE", path))

    def find_id(self, path: str, name_field: str, name: str) -> int | None:
        r = self.session.get(
            f"{self.host}{path}",
            params={"q": json.dumps({"filters": [{"col": name_field, "opr": "eq", "value": name}]})},
        )
        r.raise_for_status()
        results = r.json().get("result", [])
        return results[0]["id"] if results else None


# ---------------------------------------------------------------------------
# Database / datasets
# ---------------------------------------------------------------------------

GOLD_SCHEMA = "silver_gold"

# Какая колонка у каждого датасета является основной временной (для time-series viz).
DATASET_MAIN_DTTM = {
    "transactions_by_hour": "event_day",
    "purchases_by_hour":    "event_day",
    "revenue_daily":        "event_day",
    "cancellations_summary": "cancel_day",
    "user_cohorts":         "event_day",
    # promo_codes_analysis — без time series (snapshot)
}


def upsert_database(client: SupersetClient) -> int:
    name = "lab08-trino"
    payload = {
        "database_name": name,
        "sqlalchemy_uri": "trino://admin@trino.data-platform.svc.cluster.local:8080/hudi",
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "extra": json.dumps({"engine_params": {"connect_args": {"http_scheme": "http"}}}),
    }
    existing = client.find_id("/api/v1/database/", "database_name", name)
    if existing:
        print(f"  Database '{name}' exists (id={existing}) — updating connection.")
        client.put(f"/api/v1/database/{existing}", payload)
        return existing
    db_id = client.post("/api/v1/database/", payload)["id"]
    print(f"  Created database id={db_id}")
    return db_id


def upsert_dataset(client: SupersetClient, db_id: int, table: str) -> int | None:
    """Создаёт dataset, если его нет; затем проставляет main_dttm_col, если нужно."""
    existing = client.find_id("/api/v1/dataset/", "table_name", table)
    if existing:
        ds_id = existing
        print(f"  Dataset '{GOLD_SCHEMA}.{table}' exists (id={ds_id}).")
    else:
        r = client.session.post(
            f"{client.host}/api/v1/dataset/",
            json={"database": db_id, "schema": GOLD_SCHEMA, "table_name": table},
        )
        if r.status_code == 422 and "could not be found" in r.text:
            print(f"  Dataset '{GOLD_SCHEMA}.{table}': таблица ещё не создана в Trino — skip.")
            return None
        if not r.ok:
            print(f"  ERROR {r.status_code}: {r.text[:300]}", file=sys.stderr)
            r.raise_for_status()
        ds_id = r.json()["id"]
        print(f"  Created dataset '{GOLD_SCHEMA}.{table}' id={ds_id}")
        time.sleep(0.5)

    # Проставим main_dttm_col, если задан и колонка существует.
    # NB: в Superset 4.x колонки обновляются ТОЛЬКО через PUT /api/v1/dataset/{id}
    # с полным массивом columns; отдельного /column/{id} endpoint нет.
    dttm = DATASET_MAIN_DTTM.get(table)
    if dttm:
        cols = client.get(f"/api/v1/dataset/{ds_id}").get("result", {}).get("columns", [])
        col_names = {c["column_name"] for c in cols}
        if dttm in col_names:
            updated_cols = []
            for c in cols:
                # Передаём только поля, которые принимает DatasetColumnsPutSchema.
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
            print(f"    main_dttm_col={dttm}")
        else:
            print(f"    WARN: колонка '{dttm}' не найдена в датасете {table}, time-series viz не сработает")
    return ds_id


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def upsert_chart(client: SupersetClient, payload: dict) -> int | None:
    """Идемпотентный upsert: PUT по id если есть, иначе POST."""
    if payload.get("datasource_id") is None:
        print(f"  Chart '{payload['slice_name']}': skip (нет датасета)")
        return None
    existing = client.find_id("/api/v1/chart/", "slice_name", payload["slice_name"])
    if existing:
        # На update передаём только мутабельные поля. Менять datasource через PUT
        # рискованно (Superset 4.x на это часто отвечает 500), а нам и не нужно.
        update_payload = {
            k: v for k, v in payload.items()
            if k in {"slice_name", "description", "viz_type", "params"}
        }
        client.put(f"/api/v1/chart/{existing}", update_payload)
        print(f"  Updated chart '{payload['slice_name']}' id={existing}")
        return existing
    chart_id = client.post("/api/v1/chart/", payload)["id"]
    print(f"  Created chart '{payload['slice_name']}' id={chart_id}")
    return chart_id


def cleanup_obsolete_charts(client: SupersetClient) -> None:
    """Удаляем чарты от старых версий скрипта."""
    for stale in (
        "Daily Revenue in TGRK",
        "Promo Codes: Over-Limit Uses",
        "Purchases by Hour (Share)",
    ):
        sid = client.find_id("/api/v1/chart/", "slice_name", stale)
        if sid:
            client.delete(f"/api/v1/chart/{sid}")
            print(f"  Removed obsolete chart '{stale}' (id={sid})")


# Generic helpers for chart params -----------------------------------------

def simple_metric(column: str, agg: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": agg,
        "label": label,
    }


def sql_metric(sql: str, label: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label,
        "hasCustomLabel": True,
    }


def adhoc_filter(col: str, op: str, val: Any) -> dict:
    return {
        "expressionType": "SIMPLE",
        "subject": col,
        "operator": op,
        "comparator": val,
        "clause": "WHERE",
    }


REAL_USERS_FILTER = adhoc_filter("is_test_user", "==", False)


# ---------------------------------------------------------------------------
# Charts — definitions
# ---------------------------------------------------------------------------

def build_charts(client: SupersetClient, ds: dict[str, int | None]) -> dict[str, int | None]:
    """Возвращает словарь slice_name → chart_id."""
    charts: dict[str, int | None] = {}

    # --- KPI row (Big Number) -------------------------------------------------
    charts["KPI: Total Revenue (TGRK)"] = upsert_chart(client, {
        "slice_name": "KPI: Total Revenue (TGRK)",
        "description": "Совокупная выручка в базовой валюте TGRK по реальным пользователям. "
                       "Конвертация PUNK/RUB → TGRK через дневные курсы (forward-fill).",
        "viz_type": "big_number_total",
        "datasource_id": ds["revenue_daily"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": simple_metric("revenue_tgrk", "SUM", "Revenue, TGRK"),
            "y_axis_format": ",.0f",
            "subheader": "сумма за весь период",
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
        }),
    })

    charts["KPI: Completed %"] = upsert_chart(client, {
        "slice_name": "KPI: Completed %",
        "description": "Доля успешных транзакций (status=completed) среди всех транзакций реальных пользователей.",
        "viz_type": "big_number_total",
        "datasource_id": ds["transactions_by_hour"],
        "datasource_type": "table",
        "params": json.dumps({
            "metric": sql_metric("SUM(completed_cnt) * 1.0 / NULLIF(SUM(tx_cnt), 0)",
                                 "Completed share"),
            "adhoc_filters": [REAL_USERS_FILTER],
            "y_axis_format": ".1%",
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
        }),
    })

    # --- Trends ---------------------------------------------------------------
    charts["Daily Revenue (TGRK)"] = upsert_chart(client, {
        "slice_name": "Daily Revenue (TGRK)",
        "description": "Дневная выручка в TGRK. Конвертация валют по дневным курсам "
                       "(forward-fill последним известным).",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": ds["revenue_daily"],
        "datasource_type": "table",
        "params": json.dumps({
            "x_axis": "event_day",
            "time_grain_sqla": "P1D",
            "metrics": [simple_metric("revenue_tgrk", "SUM", "Revenue, TGRK")],
            "groupby": [],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
            "y_axis_format": ",.0f",
            "x_axis_title": "Day",
            "y_axis_title": "Revenue, TGRK",
            "show_legend": False,
            "rich_tooltip": True,
            "markerEnabled": True,
        }),
    })

    charts["Daily Transactions by Status"] = upsert_chart(client, {
        "slice_name": "Daily Transactions by Status",
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
            "y_axis_format": "SMART_NUMBER",
            "x_axis_title": "Day",
            "y_axis_title": "Transactions",
            "show_legend": True,
            "rich_tooltip": True,
        }),
    })

    # --- Operations -----------------------------------------------------------
    charts["Transactions by Hour"] = upsert_chart(client, {
        "slice_name": "Transactions by Hour",
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
            "y_axis_format": "SMART_NUMBER",
            "x_axis_label": "Hour of day",
            "y_axis_label": "Transactions",
        }),
    })

    charts["Purchases by Hour"] = upsert_chart(client, {
        "slice_name": "Purchases by Hour",
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
            "x_axis_label": "Hour of day",
            "y_axis_label": "Purchases",
        }),
    })

    charts["Cancellations by Reason (daily)"] = upsert_chart(client, {
        "slice_name": "Cancellations by Reason (daily)",
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
            "y_axis_format": "SMART_NUMBER",
            "x_axis_title": "Day",
            "y_axis_title": "Cancellations",
            "rich_tooltip": True,
        }),
    })

    # --- Users & Promo --------------------------------------------------------
    charts["User Cohorts: New vs Returning"] = upsert_chart(client, {
        "slice_name": "User Cohorts: New vs Returning",
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
            "y_axis_format": "SMART_NUMBER",
            "x_axis_title": "Day",
            "y_axis_title": "Unique users",
            "rich_tooltip": True,
            "opacity": 0.5,
        }),
    })

    charts["Promo Codes Analysis"] = upsert_chart(client, {
        "slice_name": "Promo Codes Analysis",
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


# ---------------------------------------------------------------------------
# Dashboard layout (position_json + native filters)
# ---------------------------------------------------------------------------

def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def build_position_json(tabs_spec: list[dict], chart_meta: dict[int, dict]) -> dict:
    """
    tabs_spec: [
        {"name": "Overview", "rows": [
            [{"chart_id": 1, "w": 4, "h": 10}, ...],   # одна строка дашборда
            [{"markdown": "## Header"}],
        ]},
    ]
    chart_meta: chart_id → {"name": str, "uuid": str}
    """
    pos: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    tabs_id = _new_id("TABS")
    pos[tabs_id] = {"type": "TABS", "id": tabs_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"]}
    pos["GRID_ID"]["children"].append(tabs_id)

    for tab in tabs_spec:
        tab_id = _new_id("TAB")
        pos[tab_id] = {
            "type": "TAB",
            "id": tab_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": tab["name"]},
        }
        pos[tabs_id]["children"].append(tab_id)

        for row in tab["rows"]:
            row_id = _new_id("ROW")
            pos[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", tabs_id, tab_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            pos[tab_id]["children"].append(row_id)

            for item in row:
                if "markdown" in item:
                    md_id = _new_id("MARKDOWN")
                    pos[md_id] = {
                        "type": "MARKDOWN",
                        "id": md_id,
                        "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", tabs_id, tab_id, row_id],
                        "meta": {
                            "width": item.get("w", 12),
                            "height": item.get("h", 4),
                            "code": item["markdown"],
                        },
                    }
                    pos[row_id]["children"].append(md_id)
                    continue

                cid = item["chart_id"]
                meta = chart_meta[cid]
                ch_id = _new_id("CHART")
                pos[ch_id] = {
                    "type": "CHART",
                    "id": ch_id,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", tabs_id, tab_id, row_id],
                    "meta": {
                        "width": item.get("w", 4),
                        "height": item.get("h", 50),
                        "chartId": cid,
                        "sliceName": meta["name"],
                        "uuid": meta["uuid"],
                    },
                }
                pos[row_id]["children"].append(ch_id)
    return pos


def build_native_filters(
    test_user_dataset_id: int | None,
    chart_ids_with_test_user: list[int],
    all_chart_ids: list[int],
    chart_ids_without_time: list[int],
) -> list[dict]:
    """Глобальные фильтры дашборда: event_day и is_test_user.

    Scope формируем через `excluded` — это chart_id'ы, к которым фильтр НЕ применяется.
    Так Superset не пытается добавить WHERE в чарты, где нужной колонки нет.
    """
    filters: list[dict] = []

    excluded_for_time = chart_ids_without_time
    filters.append({
        "id": "NATIVE_FILTER-event-day",
        "name": "Event day",
        "filterType": "filter_time",
        "type": "NATIVE_FILTER",
        "targets": [{}],
        "controlValues": {"enableEmptyFilter": False},
        "defaultDataMask": {
            "extraFormData": {"time_range": "Last 30 days"},
            "filterState": {"value": "Last 30 days"},
        },
        "cascadeParentIds": [],
        "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded_for_time},
    })

    if test_user_dataset_id is not None and chart_ids_with_test_user:
        excluded_for_test_user = [c for c in all_chart_ids if c not in chart_ids_with_test_user]
        filters.append({
            "id": "NATIVE_FILTER-is-test-user",
            "name": "Test users",
            "filterType": "filter_select",
            "type": "NATIVE_FILTER",
            "targets": [{
                "datasetId": test_user_dataset_id,
                "column": {"name": "is_test_user"},
            }],
            "controlValues": {
                "multiSelect": False,
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "inverseSelection": False,
                "searchAllOptions": False,
            },
            "defaultDataMask": {
                "extraFormData": {"filters": [{"col": "is_test_user", "op": "IN", "val": [False]}]},
                "filterState": {"value": [False]},
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded_for_test_user},
        })

    return filters


def upsert_dashboard(
    client: SupersetClient,
    title: str,
    charts_by_name: dict[str, int | None],
    ds: dict[str, int | None],
) -> int:
    # Резолвим chart_id и метаданные (uuid).
    valid_charts: dict[str, int] = {n: cid for n, cid in charts_by_name.items() if cid is not None}
    chart_meta: dict[int, dict] = {}
    for name, cid in valid_charts.items():
        info = client.get(f"/api/v1/chart/{cid}").get("result", {})
        chart_meta[cid] = {"name": name, "uuid": info.get("uuid") or str(uuid.uuid4())}

    def cid(name: str) -> int | None:
        return valid_charts.get(name)

    # Layout: 3 таба.
    kpis = [n for n in [
        "KPI: Total Revenue (TGRK)",
        "KPI: Total Transactions",
        "KPI: Completed %",
        "KPI: Cancellations",
        "KPI: User-days (DAU sum)",
    ] if cid(n)]

    tabs_spec: list[dict] = [
        {
            "name": "Overview",
            "rows": [
                [{"markdown": "### Lab08 — Transaction Analytics\n"
                              "Витрины gold-слоя поверх Hudi/Trino. Фильтры дашборда: "
                              "период `event_day` и `is_test_user` (по умолчанию — только real users).",
                  "w": 12, "h": 4}],
                [{"chart_id": cid(n), "w": 12 // max(len(kpis), 1), "h": 28} for n in kpis],
                [
                    {"chart_id": cid("Daily Revenue (TGRK)"), "w": 6, "h": 50},
                    {"chart_id": cid("Daily Transactions by Status"), "w": 6, "h": 50},
                ],
            ],
        },
        {
            "name": "Operations",
            "rows": [
                [{"markdown": "### Операционные разрезы\n"
                              "Профиль активности по часам суток + структура отмен.", "w": 12, "h": 3}],
                [
                    {"chart_id": cid("Transactions by Hour"), "w": 6, "h": 50},
                    {"chart_id": cid("Purchases by Hour"), "w": 6, "h": 50},
                ],
                [{"chart_id": cid("Cancellations by Reason (daily)"), "w": 12, "h": 50}],
            ],
        },
        {
            "name": "Users & Promo",
            "rows": [
                [{"markdown": "### Пользователи и промокоды\n"
                              "Когортное распределение DAU + детализация по промокодам.", "w": 12, "h": 3}],
                [{"chart_id": cid("User Cohorts: New vs Returning"), "w": 12, "h": 50}],
                [{"chart_id": cid("Promo Codes Analysis"), "w": 12, "h": 60}],
            ],
        },
    ]

    # Чистим строки с дырами (если каких-то чартов нет — выкидываем None).
    for tab in tabs_spec:
        cleaned_rows = []
        for row in tab["rows"]:
            row2 = [it for it in row if it.get("markdown") or it.get("chart_id") is not None]
            if row2:
                cleaned_rows.append(row2)
        tab["rows"] = cleaned_rows

    position = build_position_json(tabs_spec, chart_meta)

    # Scope фильтров: is_test_user применяем только к чартам на transactions_by_hour;
    # Event day исключаем у чартов без временной колонки (Promo Codes).
    charts_with_test_user = [
        c for n, c in valid_charts.items()
        if n in {
            "KPI: Total Transactions",
            "KPI: Completed %",
            "Daily Transactions by Status",
            "Transactions by Hour",
        }
    ]
    charts_without_time = [
        c for n, c in valid_charts.items() if n in {"Promo Codes Analysis"}
    ]
    json_metadata = {
        "refresh_frequency": 0,
        "color_scheme": "supersetColors",
        "native_filter_configuration": build_native_filters(
            test_user_dataset_id=ds.get("transactions_by_hour"),
            chart_ids_with_test_user=charts_with_test_user,
            all_chart_ids=list(valid_charts.values()),
            chart_ids_without_time=charts_without_time,
        ),
        "cross_filters_enabled": True,
    }

    payload = {
        "dashboard_title": title,
        "published": True,
        "position_json": json.dumps(position),
        "json_metadata": json.dumps(json_metadata),
    }

    existing = client.find_id("/api/v1/dashboard/", "dashboard_title", title)
    if existing:
        dash_id = existing
        client.put(f"/api/v1/dashboard/{dash_id}", payload)
        print(f"  Updated dashboard '{title}' id={dash_id}")
    else:
        dash_id = client.post("/api/v1/dashboard/", payload)["id"]
        print(f"  Created dashboard '{title}' id={dash_id}")

    # Привязываем чарты к дашборду. Без этого frontend показывает в layout
    # "There is no chart definition associated with this component".
    # Делаем со sleep чтобы не упереться в Superset rate-limiter (429 на массовых PUT).
    for cid_ in valid_charts.values():
        client.put(f"/api/v1/chart/{cid_}", {"dashboards": [dash_id]})
        time.sleep(0.4)
    print(f"  Linked {len(valid_charts)} charts to dashboard id={dash_id}")
    return dash_id


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    print(f"Connecting to Superset at {args.host} as {args.user}...")
    client = SupersetClient(args.host, args.user, args.password)

    print("\n[1/4] Database connection...")
    db_id = upsert_database(client)

    print("\n[2/4] Datasets...")
    tables = [
        "transactions_by_hour",
        "purchases_by_hour",
        "revenue_daily",
        "promo_codes_analysis",
        "cancellations_summary",
        "user_cohorts",
    ]
    ds = {t: upsert_dataset(client, db_id, t) for t in tables}

    print("\n[3/4] Charts...")
    cleanup_obsolete_charts(client)
    charts = build_charts(client, ds)

    print("\n[4/4] Dashboard...")
    if not any(charts.values()):
        print("  Нет ни одного чарта (gold таблицы пока пусты). Запусти DAG transactions_pipeline и повтори.")
        return
    upsert_dashboard(client, "Lab08 — Transaction Analytics", charts, ds)

    print("\nDone! Open Superset → Dashboards → 'Lab08 — Transaction Analytics'.")


if __name__ == "__main__":
    main()

"""Dashboard layout (``position_json``), native filters and upsert."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from ._log import log
from .client import SupersetClient


def _new_id(prefix: str) -> str:
    """Return a Superset dashboard component id with the given prefix."""
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def build_position_json(tabs_spec: list[dict], chart_meta: dict[int, dict]) -> dict:
    """Build the Superset ``position_json`` for a tabbed dashboard.

    Args:
        tabs_spec: Tab definitions; each tab has ``name`` and ``rows``,
            where every row is a list of items describing either a chart
            (``chart_id``, optional ``w``, ``h``) or a markdown block
            (``markdown``, optional ``w``, ``h``).
        chart_meta: Mapping ``chart_id -> {"name": str, "uuid": str}``.

    Returns:
        Position JSON dict ready to be serialised for Superset.
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
    """Build dashboard-level native filters for ``event_day`` and ``is_test_user``.

    Scope is expressed via ``excluded`` chart ids so Superset only injects
    each filter into charts whose datasets actually expose the column.

    Args:
        test_user_dataset_id: Dataset id used as the ``is_test_user`` source.
        chart_ids_with_test_user: Charts that support the ``is_test_user`` filter.
        all_chart_ids: All chart ids on the dashboard.
        chart_ids_without_time: Charts that do not have a time column.

    Returns:
        ``native_filter_configuration`` list for ``json_metadata``.
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
    """Create or update the Lab08 dashboard and link all charts to it.

    Args:
        client: Authenticated Superset client.
        title: Dashboard title (also used as the lookup key).
        charts_by_name: Mapping ``slice_name`` -> ``chart_id`` (``None``
            entries are skipped).
        ds: Mapping of gold table name to dataset id; used to resolve
            the ``is_test_user`` filter target.

    Returns:
        Id of the upserted dashboard.
    """
    valid_charts: dict[str, int] = {n: cid for n, cid in charts_by_name.items() if cid is not None}
    chart_meta: dict[int, dict] = {}
    for name, cid in valid_charts.items():
        info = client.get(f"/api/v1/chart/{cid}").get("result", {})
        chart_meta[cid] = {"name": name, "uuid": info.get("uuid") or str(uuid.uuid4())}

    def cid(name: str) -> int | None:
        return valid_charts.get(name)

    kpis = [n for n in [
        "KPI: Total Revenue (TGRK)",
        "KPI: Total Transactions",
        "KPI: Invalid Refund %",
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
                  "w": 12, "h": 10}],
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
                              "Профиль активности по часам суток + структура отмен.", "w": 12, "h": 10}],
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
                              "Когортное распределение DAU + детализация по промокодам.", "w": 12, "h": 10}],
                [{"chart_id": cid("User Cohorts: New vs Returning"), "w": 12, "h": 50}],
                [{"chart_id": cid("Promo Codes Analysis"), "w": 12, "h": 50}],
            ],
        },
    ]

    for tab in tabs_spec:
        cleaned_rows = []
        for row in tab["rows"]:
            row2 = [it for it in row if it.get("markdown") or it.get("chart_id") is not None]
            if row2:
                cleaned_rows.append(row2)
        tab["rows"] = cleaned_rows

    position = build_position_json(tabs_spec, chart_meta)

    charts_with_test_user = [
        c for n, c in valid_charts.items()
        if n in {
            "KPI: Total Transactions",
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
        log.info("Updated dashboard '%s' id=%s", title, dash_id)
    else:
        dash_id = client.post("/api/v1/dashboard/", payload)["id"]
        log.info("Created dashboard '%s' id=%s", title, dash_id)

    for cid_ in valid_charts.values():
        client.put(f"/api/v1/chart/{cid_}", {"dashboards": [dash_id]})
        time.sleep(0.4)
    log.info("Linked %d charts to dashboard id=%s", len(valid_charts), dash_id)
    return dash_id

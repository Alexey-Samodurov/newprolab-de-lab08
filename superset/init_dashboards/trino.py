"""Trino database connection upsert for the Lab08 Superset bootstrap."""

from __future__ import annotations

import json

from ._log import log
from .client import SupersetClient


DATABASE_NAME = "lab08-trino"
SQLALCHEMY_URI = "trino://admin@trino.data-platform.svc.cluster.local:8080/hudi"


def upsert_database(client: SupersetClient) -> int:
    """Create or refresh the Trino database connection in Superset.

    Args:
        client: Authenticated Superset client.

    Returns:
        Id of the upserted database row.
    """
    payload = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "extra": json.dumps({"engine_params": {"connect_args": {"http_scheme": "http"}}}),
    }
    existing = client.find_id("/api/v1/database/", "database_name", DATABASE_NAME)
    if existing:
        log.info("Database '%s' exists (id=%s) — updating connection.", DATABASE_NAME, existing)
        client.put(f"/api/v1/database/{existing}", payload)
        return existing
    db_id = client.post("/api/v1/database/", payload)["id"]
    log.info("Created database id=%s", db_id)
    return db_id

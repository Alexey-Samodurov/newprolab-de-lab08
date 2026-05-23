"""Process-wide logger for the Superset bootstrap package."""

from __future__ import annotations

import logging
import os
import sys


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)

log = logging.getLogger("init_dashboards")

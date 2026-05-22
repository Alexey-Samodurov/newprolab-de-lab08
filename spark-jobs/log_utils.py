from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


def configure_logging(level: str | int | None = None) -> None:
    """Configure the root logger once per process.

    Idempotent: subsequent calls do not add another handler. The level
    falls back to the ``LOG_LEVEL`` env var or ``INFO``. Logs go to
    ``stdout`` to avoid mixing with Spark/Hudi stderr output.

    Args:
        level: Optional logging level (name or numeric).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    lvl = level or os.environ.get("LOG_LEVEL", "INFO")
    if isinstance(lvl, str):
        lvl = lvl.upper()
    root = logging.getLogger()
    root.setLevel(lvl)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root.handlers = [handler]
    logging.getLogger("py4j").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module logger, configuring root logging on first use.

    Args:
        name: Logger name (typically ``__name__``).

    Returns:
        Configured ``logging.Logger`` instance.
    """
    configure_logging()
    return logging.getLogger(name)

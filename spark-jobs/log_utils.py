from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


def configure_logging(level: str | int | None = None) -> None:
    """Configure root logging once per process.

    Идёмпотентно: повторные вызовы не добавляют второй handler. Уровень
    берётся из аргумента, иначе из ``LOG_LEVEL`` env, иначе ``INFO``.
    Лог летит в ``stdout`` (а не stderr), чтобы не дублироваться с
    Spark/Hudi-логами и нормально собирался k8s log-pipeline-ом.
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
    """Return a module logger, configuring root logging on first use."""
    configure_logging()
    return logging.getLogger(name)

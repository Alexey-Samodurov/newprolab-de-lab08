from __future__ import annotations

import json

from pyspark.sql import SparkSession


def read_latest_commit(
    spark: SparkSession,
    hudi_path: str,
    prev_instant: str | None,
) -> tuple[str | None, list[str], int]:
    """Read the latest completed Hudi commit metadata from S3.

    Returns ``(instant, partitions, rows_written)`` where ``partitions`` is the
    raw set of keys from ``partitionToWriteStats`` (e.g. ``"event_day=2024-01-01"``
    or ``""`` for non-partitioned tables) and ``rows_written`` is the sum of
    ``numWrites + numUpdateWrites`` across all write stats.

    If no new ``.commit`` instant exists since ``prev_instant`` (or the table
    has no timeline yet), returns ``(latest_or_None, [], 0)``.

    Чтение идёт напрямую через Hadoop FS — без Spark job-а, поэтому метрики
    батча получаем «бесплатно», не запуская count/distinct по данным.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(hudi_path.rstrip("/") + "/.hoodie")
    fs = base.getFileSystem(hconf)
    if not fs.exists(base):
        return None, [], 0

    statuses = fs.listStatus(base)
    latest_ts: str | None = None
    latest_path = None
    for s in statuses:
        name = s.getPath().getName()
        if name.endswith(".commit"):
            ts = name[: -len(".commit")]
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_path = s.getPath()

    if latest_ts is None or latest_ts == prev_instant:
        return latest_ts, [], 0

    in_ = fs.open(latest_path)
    try:
        baos = jvm.java.io.ByteArrayOutputStream()
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(in_, baos, 4096, False)
        content = baos.toString("UTF-8")
    finally:
        in_.close()

    md = json.loads(content)
    p2s = md.get("partitionToWriteStats") or {}
    parts = list(p2s.keys())
    rows = 0
    for stats in p2s.values():
        for st in stats or []:
            rows += int(st.get("numWrites") or 0)
            rows += int(st.get("numUpdateWrites") or 0)
    return latest_ts, parts, rows


def normalize_partitions(
    raw_parts: list[str],
    *,
    nonpartitioned_marker: str = "__nonpartitioned__",
    out_prefix: str = "day=",
) -> list[str]:
    """Normalize raw Hudi partition keys to the watermark's ``day=<value>`` format.

    Hudi с ``hive_style_partitioning=true`` пишет ключи вида
    ``event_day=2024-01-01``. Watermark-таблица исторически хранит их как
    ``day=<value>`` — здесь срезаем имя колонки и подставляем фиксированный
    префикс, чтобы не ломать обратную совместимость с уже накопленными
    записями.
    """
    out: set[str] = set()
    for p in raw_parts:
        if not p:
            out.add(nonpartitioned_marker)
            continue
        value = p.split("=", 1)[1] if "=" in p else p
        out.add(f"{out_prefix}{value}")
    return sorted(out)

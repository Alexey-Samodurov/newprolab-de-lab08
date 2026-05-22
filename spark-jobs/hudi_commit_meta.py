from __future__ import annotations

import json

from pyspark.sql import SparkSession


def read_latest_commit(
    spark: SparkSession,
    hudi_path: str,
    prev_instant: str | None,
) -> tuple[str | None, list[str], int]:
    """Read the latest completed Hudi commit metadata from S3.

    Reads directly via Hadoop FS (no Spark job), so batch metrics are
    obtained without scanning data.

    Args:
        spark: Active SparkSession.
        hudi_path: Base path of the Hudi table.
        prev_instant: Previously observed instant; if it matches the
            latest, no new metadata is returned.

    Returns:
        Tuple ``(instant, partitions, rows_written)`` where ``partitions``
        contains raw ``partitionToWriteStats`` keys (e.g.
        ``"event_day=2024-01-01"`` or ``""``) and ``rows_written`` is the
        sum of ``numWrites + numUpdateWrites``. Returns
        ``(latest_or_None, [], 0)`` when nothing new is available.
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

    Hudi with ``hive_style_partitioning=true`` emits keys like
    ``event_day=2024-01-01``; the watermark table historically stores them
    as ``day=<value>``. This strips the column name and prepends a fixed
    prefix to preserve backward compatibility.

    Args:
        raw_parts: Raw partition keys from ``partitionToWriteStats``.
        nonpartitioned_marker: Marker used when a key is empty.
        out_prefix: Prefix prepended to extracted partition values.

    Returns:
        Sorted list of normalized partition keys.
    """
    out: set[str] = set()
    for p in raw_parts:
        if not p:
            out.add(nonpartitioned_marker)
            continue
        value = p.split("=", 1)[1] if "=" in p else p
        out.add(f"{out_prefix}{value}")
    return sorted(out)

from __future__ import annotations

import argparse
import sys
import time
import traceback

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

from utils.hudi import reference_hudi_opts, write_hudi
from utils.log import get_logger


log = get_logger(__name__)
USERS_SCHEMA = StructType([
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registered_at", LongType(), True),
    StructField("is_test_user", BooleanType(), True),
])

TEST_USERS_SCHEMA = StructType([
    StructField("test_user_uuid", StringType(), True),
])

PROMO_CODES_SCHEMA = StructType([
    StructField("promo_code_id", LongType(), True),
    StructField("code", StringType(), True),
    StructField("max_uses", LongType(), True),
    StructField("expiry_date", StringType(), True),
])


SPECS: list[tuple[str, str, StructType, str]] = [
    ("users.jsonl",       "users",       USERS_SCHEMA,       "user_id"),
    ("test_users.jsonl",  "test_users",  TEST_USERS_SCHEMA,  "test_user_uuid"),
    ("promo_codes.jsonl", "promo_codes", PROMO_CODES_SCHEMA, "promo_code_id"),
]


def overwrite_reference(
    spark: SparkSession,
    *,
    src_root: str,
    fname: str,
    table: str,
    schema: StructType,
    pk: str,
    batch_id: int,
) -> None:
    """Overwrite a reference Hudi table from a JSON snapshot.

    Args:
        spark: Active SparkSession.
        src_root: S3a prefix containing reference snapshots.
        fname: JSON file name within ``src_root``.
        table: Destination Hudi table name.
        schema: Schema enforced on the source file.
        pk: Recordkey column.
        batch_id: Batch identifier used for logging.
    """
    path = f"{src_root.rstrip('/')}/{fname}"
    df = (
        spark.read.schema(schema).json(path)
        .withColumn("ingested_at", F.current_timestamp())
    )
    opts = reference_hudi_opts(table, "bronze", pk=pk)
    write_hudi(df, opts)
    log.info("ref table=%s overwrote from %s (batch_id=%s)", table, fname, batch_id)


def main() -> int:
    """Run the reference batch job.

    Returns:
        0 on success, 1 if no tables matched the whitelist or any table
        failed to load in strict mode.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reference-path", required=True,
        help="S3a prefix with reference files (e.g. s3a://npl-de18-lab8-data/reference/)",
    )
    parser.add_argument(
        "--tables", default="",
        help="Optional comma-separated whitelist (users,test_users,promo_codes). Empty = all.",
    )
    parser.add_argument(
        "--strict", dest="strict", action="store_true", default=True,
        help="Fail if any file is missing or empty (default).",
    )
    parser.add_argument(
        "--no-strict", dest="strict", action="store_false",
        help="Log missing files and continue.",
    )
    args = parser.parse_args()

    whitelist = {t.strip() for t in args.tables.split(",") if t.strip()}
    selected = [s for s in SPECS if not whitelist or s[1] in whitelist]
    if not selected:
        log.error("no tables match whitelist=%s", whitelist)
        return 1

    spark = (
        SparkSession.builder
        .appName("bronze-reference-batch")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.hive.metastore.client.socket.timeout", "600s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    log.info("reference_path=%s tables=%s strict=%s",
             args.reference_path, [s[1] for s in selected], args.strict)

    batch_id = int(time.time())
    failures: list[tuple[str, str]] = []
    src_root = args.reference_path

    for fname, table, schema, pk in selected:
        try:
            overwrite_reference(
                spark, src_root=src_root, fname=fname, table=table,
                schema=schema, pk=pk, batch_id=batch_id,
            )
        except Exception as exc:
            tb = traceback.format_exc()
            extra = ""
            for attr in ("error_class", "message_parameters", "errorClass", "getErrorClass"):
                v = getattr(exc, attr, None)
                if callable(v):
                    try:
                        v = v()
                    except Exception:
                        v = None
                if v:
                    extra += f" {attr}={v!r}"
            log.error("ref table=%s FAILED: type=%s str=%s repr=%r%s",
                      table, type(exc).__name__, exc, exc, extra)
            log.error("%s", tb)
            if not args.strict:
                if "Path does not exist" in tb or "FileNotFoundException" in tb:
                    log.warning("ref table=%s (no-strict) treating as missing file, continuing", table)
                    continue
            failures.append((table, str(exc) or repr(exc) or type(exc).__name__))

    spark.stop()

    if failures:
        for t, e in failures:
            log.error("failure detail [%s]: %s", t, e)
        log.error("FAILED tables: %s", [t for t, _ in failures])
        return 1
    log.info("all reference tables overwritten successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

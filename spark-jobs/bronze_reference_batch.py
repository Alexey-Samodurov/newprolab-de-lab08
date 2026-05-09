from __future__ import annotations

import argparse
import sys
import time
import traceback

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

from hudi_utils import reference_hudi_opts, write_hudi
from watermark_utils import bootstrap_watermark_table, write_watermark


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
) -> int:
    """Перезаписать одну reference-таблицу. Возвращает число строк."""
    path = f"{src_root.rstrip('/')}/{fname}"
    df = (
        spark.read.schema(schema).json(path)
        .withColumn("ingested_at", F.current_timestamp())
    )
    rows = df.count()
    if rows == 0:
        print(f"[ref:{table}] WARN empty snapshot at {path}, skipping overwrite")
        return 0

    opts = reference_hudi_opts(table, "bronze", pk=pk)
    write_hudi(df, opts)
    write_watermark(
        spark,
        table_name=f"reference_{table}",
        partitions=["__nonpartitioned__"],
        rows_in_batch=rows,
        batch_id=batch_id,
    )
    print(f"[ref:{table}] overwrote from {fname} rows={rows}")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reference-path", required=True,
        help="S3a-префикс с reference-файлами (e.g. s3a://npl-de18-lab8-data/reference/)",
    )
    parser.add_argument(
        "--tables", default="",
        help="Опциональный whitelist через запятую (users,test_users,promo_codes). "
             "Пусто = все.",
    )
    parser.add_argument(
        "--strict", dest="strict", action="store_true", default=True,
        help="Падать если хотя бы один файл недоступен/пустой (default).",
    )
    parser.add_argument(
        "--no-strict", dest="strict", action="store_false",
        help="Отсутствующие файлы логировать и пропускать.",
    )
    args = parser.parse_args()

    whitelist = {t.strip() for t in args.tables.split(",") if t.strip()}
    selected = [s for s in SPECS if not whitelist or s[1] in whitelist]
    if not selected:
        print(f"[main] no tables match whitelist={whitelist}", file=sys.stderr)
        return 1

    spark = (
        SparkSession.builder
        .appName("bronze-reference-batch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[main] reference_path={args.reference_path} tables={[s[1] for s in selected]} strict={args.strict}")

    bootstrap_watermark_table(spark)

    batch_id = int(time.time())
    failures: list[tuple[str, str]] = []
    src_root = args.reference_path

    for fname, table, schema, pk in selected:
        try:
            overwrite_reference(
                spark, src_root=src_root, fname=fname, table=table,
                schema=schema, pk=pk, batch_id=batch_id,
            )
        except Exception as exc:  # noqa: BLE001
            # PySparkValueError и Py4JJavaError часто дают пустой repr() —
            # тащим полный traceback и нагрузку класса.
            tb = traceback.format_exc()
            extra = ""
            for attr in ("error_class", "message_parameters", "errorClass", "getErrorClass"):
                v = getattr(exc, attr, None)
                if callable(v):
                    try:
                        v = v()
                    except Exception:  # noqa: BLE001
                        v = None
                if v:
                    extra += f" {attr}={v!r}"
            print(f"[ref:{table}] FAILED: type={type(exc).__name__} str={exc!s} repr={exc!r}{extra}", file=sys.stderr)
            print(tb, file=sys.stderr)
            if not args.strict:
                if "Path does not exist" in tb or "FileNotFoundException" in tb:
                    print(f"[ref:{table}] (no-strict) treating as missing file, continuing")
                    continue
            failures.append((table, str(exc) or repr(exc) or type(exc).__name__))

    spark.stop()

    if failures:
        for t, e in failures:
            print(f"[main] failure detail [{t}]: {e}", file=sys.stderr)
        print(f"[main] FAILED tables: {[t for t, _ in failures]}", file=sys.stderr)
        return 1
    print("[main] all reference tables overwritten successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

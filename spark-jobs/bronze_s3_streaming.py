"""
Bronze ingest из публичного S3 как long-running Spark Structured Streaming.

Заменяет batch-DAG `bronze_s3_*`. Один long-running SparkApplication поднимает
4 параллельных стрима:

  * transactions   — file source, partitioned by event_day, composite_pk
  * cancellations  — file source, partitioned by event_day
  * exchange_rates — file source, без партиций
  * reference      — file source на каталог reference/ (3 файла: users / test_users / promo_codes),
                     внутри foreachBatch разводим по таблицам через _input_file_name.

Гарантии:
  * exactly-once на уровне обработки файлов (Spark file source ведёт checkpoint на S3);
  * upsert по PK закрывает дубли и поздние перезаливки исходного файла;
  * driver/executor падает → restartPolicy: Always → стрим продолжает с offset из checkpoint.
"""
import sys
from functools import partial

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType,
)

from hudi_utils import hudi_opts, write_hudi


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

TX_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("promo_code_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
])

CANCEL_SCHEMA = StructType([
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])

RATES_SCHEMA = StructType([
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
])

# Reference: union-схема всех reference-файлов. Streaming JSON source требует
# явную схему (без spark.sql.streaming.schemaInference=true он стартовать не будет).
# В foreachBatch мы перечитываем файл со СВОЕЙ схемой по имени — а сюда отдаём union.
REFERENCE_UNION_SCHEMA = StructType([
    # users.jsonl
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registered_at", LongType(), True),
    StructField("is_test_user", BooleanType(), True),
    # test_users.jsonl
    StructField("test_user_uuid", StringType(), True),
    # promo_codes.jsonl
    StructField("promo_code_id", LongType(), True),
    StructField("code", StringType(), True),
    StructField("max_uses", LongType(), True),
    StructField("expiry_date", StringType(), True),
])

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


# ---------------------------------------------------------------------------
# Hudi helpers (общая реализация в spark-jobs/hudi_utils.py)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Per-stream foreachBatch handlers
# ---------------------------------------------------------------------------

def handle_transactions(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[tx batch={batch_id}] empty")
        return
    df = (batch_df
          .withColumn("created_ts", F.to_timestamp(F.from_unixtime("created_at")))
          .withColumn("event_day", F.date_format("created_ts", "yyyy-MM-dd"))
          .withColumn("composite_pk",
                      F.concat_ws("|",
                                  F.col("transaction_id").cast("string"),
                                  F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
                                  F.coalesce(F.col("user_id").cast("string"), F.lit("0"))))
          .withColumn("ingested_at", F.current_timestamp()))
    write_hudi(df, hudi_opts(
        "transactions", "bronze",
        pk="composite_pk",
        partition_field="event_day",
        precombine="ingested_at",
        # column-stats: поля, по которым реально фильтруют запросы вверх по pipeline
        # (dbt incremental WHERE event_day >=, dashboard фильтры по статусу/типу).
        column_stats_cols="event_day,status,transaction_type,ingested_at",
    ))
    print(f"[tx batch={batch_id}] rows={df.count()}")


def handle_cancellations(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[cancel batch={batch_id}] empty")
        return
    df = (batch_df
          .withColumn("cancelled_ts", F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm"))
          .withColumn("event_day", F.date_format("cancelled_ts", "yyyy-MM-dd"))
          .withColumn("ingested_at", F.current_timestamp()))
    write_hudi(df, hudi_opts(
        "cancellations", "bronze",
        pk="cancellation_id",
        partition_field="event_day",
        precombine="ingested_at",
        column_stats_cols="event_day,reason,ingested_at",
    ))
    print(f"[cancel batch={batch_id}] rows={df.count()}")


def handle_rates(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[rates batch={batch_id}] empty")
        return
    df = batch_df.withColumn("ingested_at", F.current_timestamp())
    write_hudi(df, hudi_opts(
        "exchange_rates", "bronze",
        pk="update_id",
        partition_field="",
        precombine="timestamp",
        # маленькая таблица: record-index не нужен (overhead > benefit),
        # column-stats тоже минимальный.
        column_stats_cols="timestamp",
        enable_record_index=False,
    ))
    print(f"[rates batch={batch_id}] rows={df.count()}")


def handle_reference(spark: SparkSession, src_root: str, batch_df: DataFrame, batch_id: int) -> None:
    """Reference-файлы: на каждый изменённый файл делаем upsert в свою bronze-таблицу.

    Особенности:
      * Spark file source трекает (path, modificationTime). Если upstream перезалил
        файл с новым mtime — батч получит запись и мы перечитаем файл целиком.
      * Семантика upsert: ушедшие из snapshot записи в bronze останутся.
        Для лабы это ок (silver всё равно фильтрует по флагам); для production
        нужен periodic full-overwrite job.
    """
    if batch_df.rdd.isEmpty():
        print(f"[ref batch={batch_id}] empty")
        return
    # _metadata.file_path — стабильный API в Spark 3.3+, в отличие от input_file_name().
    files = [
        r.path for r in
        batch_df.select(F.col("_source_path").alias("path")).distinct().collect()
    ]
    print(f"[ref batch={batch_id}] files={files}")

    specs = {
        "users.jsonl":       ("users",       USERS_SCHEMA,       "user_id"),
        "test_users.jsonl":  ("test_users",  TEST_USERS_SCHEMA,  "test_user_uuid"),
        "promo_codes.jsonl": ("promo_codes", PROMO_CODES_SCHEMA, "promo_code_id"),
    }
    for path in files:
        fname = path.rsplit("/", 1)[-1]
        spec = specs.get(fname)
        if not spec:
            print(f"[ref batch={batch_id}] skip unknown file {fname}")
            continue
        table, schema, pk = spec
        df = (spark.read.schema(schema).json(path)
              .withColumn("ingested_at", F.current_timestamp()))
        write_hudi(df, hudi_opts(
            table, "bronze",
            pk=pk, partition_field="", precombine="ingested_at",
            # справочники маленькие, record-index не оправдан.
            column_stats_cols="ingested_at",
            enable_record_index=False,
        ))
        print(f"[ref batch={batch_id}] upserted {table} from {fname}")


# ---------------------------------------------------------------------------
# Stream builder
# ---------------------------------------------------------------------------

def start_file_stream(
    spark: SparkSession,
    *,
    name: str,
    path: str,
    schema: StructType | None,
    glob: str,
    handler,
    checkpoint: str,
    trigger_seconds: int = 120,
    max_files_per_trigger: int = 50,
    include_source_path: bool = False,
):
    reader = (spark.readStream
              .format("json")
              .option("pathGlobFilter", glob)
              .option("maxFilesPerTrigger", str(max_files_per_trigger))
              # latestFirst=false → бэк-фил исторических файлов в первую очередь, потом новые
              .option("latestFirst", "false")
              # recursive нужно чтобы пройти day=*/slot=*/...
              .option("recursiveFileLookup", "true"))
    if schema is not None:
        reader = reader.schema(schema)

    df = reader.load(path)
    # `_metadata` — скрытая колонка file source (Spark 3.3+). Она не попадает
    # в `select("*")` и не доходит до foreachBatch автоматически — надо явно
    # спроецировать нужное поле в схему стрима.
    if include_source_path:
        df = df.select("*", F.col("_metadata.file_path").alias("_source_path"))

    query = (df.writeStream
             .queryName(name)
             .foreachBatch(handler)
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=f"{trigger_seconds} seconds")
             .start())
    print(f"[stream:{name}] started; src={path} ckpt={checkpoint}")
    return query


def main() -> None:
    src_root = sys.argv[1] if len(sys.argv) > 1 else "s3a://lake/raw"
    ckpt_root = sys.argv[2] if len(sys.argv) > 2 else "s3a://hudi/.checkpoints/bronze-s3-stream"

    spark = (SparkSession.builder
             .appName("bronze-s3-streaming")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        start_file_stream(
            spark, name="transactions",
            path=f"{src_root}/batch",
            schema=TX_SCHEMA,
            glob="transactions.jsonl",
            handler=handle_transactions,
            checkpoint=f"{ckpt_root}/transactions",
        ),
        start_file_stream(
            spark, name="cancellations",
            path=f"{src_root}/cancellations",
            schema=CANCEL_SCHEMA,
            glob="cancellations.jsonl",
            handler=handle_cancellations,
            checkpoint=f"{ckpt_root}/cancellations",
        ),
        start_file_stream(
            spark, name="rates",
            path=f"{src_root}/exchange_rates",
            schema=RATES_SCHEMA,
            glob="rates.jsonl",
            handler=handle_rates,
            checkpoint=f"{ckpt_root}/rates",
        ),
        start_file_stream(
            spark, name="reference",
            path=f"{src_root}/reference",
            schema=REFERENCE_UNION_SCHEMA,  # union — streaming source требует явную схему
            glob="*.jsonl",
            handler=partial(handle_reference, spark, src_root),
            checkpoint=f"{ckpt_root}/reference",
            trigger_seconds=300,  # справочники меняются редко
            include_source_path=True,
        ),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

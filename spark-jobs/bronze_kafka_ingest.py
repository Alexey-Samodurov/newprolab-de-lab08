"""
Bronze ingest из Kafka (Structured Streaming).
Читает топик lab08_transactions и диспатчит по полю _source в три bronze Hudi таблицы.

Стратегия одного драйвера/одного стрима:
  - Один readStream с kafka source.
  - foreachBatch в каждом микробатче делит DF по _source и пишет три таблицы Hudi отдельно.
  - Триггер ProcessingTime 60 секунд (компромисс задержка/частота коммитов).

Чекпойнты — общий путь, поскольку один query / один writer.
"""
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


EVENT_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_uuid", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("promo_code_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("cancellation_id", LongType(), True),
    StructField("original_transaction_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("cancelled_at", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("update_id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("rate_tgrk_punk", DoubleType(), True),
    StructField("rate_tgrk_rub", DoubleType(), True),
    StructField("_source", StringType(), True),
])


def hudi_opts(table, db, pk, partition_field, precombine):
    opts = {
        "hoodie.table.name": f"{db}_{table}_kafka",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": pk,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partition_field or "",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.database": db,
        "hoodie.datasource.hive_sync.table": f"{table}_kafka",
        "hoodie.datasource.hive_sync.partition_fields": partition_field or "",
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
            if partition_field
            else "org.apache.hudi.hive.NonPartitionedExtractor",
        "hoodie.metadata.enable": "true",
        "path": f"s3a://lake/{db}/{table}_kafka",
    }
    return opts


def write_hudi(df, opts):
    if df.rdd.isEmpty():
        return
    w = df.write.format("hudi")
    for k, v in opts.items():
        w = w.option(k, v)
    w.mode("append").save()


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.persist()

    tx = (batch_df.filter(F.col("_source") == "transaction")
          .withColumn("event_day", F.date_format(F.to_timestamp(F.from_unixtime("created_at")), "yyyy-MM-dd"))
          .withColumn("composite_pk",
                      F.concat_ws("|",
                                  F.col("transaction_id").cast("string"),
                                  F.coalesce(F.col("created_at").cast("string"), F.lit("0")),
                                  F.coalesce(F.col("user_id").cast("string"), F.lit("0"))))
          .withColumn("ingested_at", F.current_timestamp()))
    write_hudi(tx, hudi_opts("transactions", "bronze",
                             pk="composite_pk", partition_field="event_day",
                             precombine="ingested_at"))

    cancel = (batch_df.filter(F.col("_source") == "cancellation")
              .withColumn("cancelled_ts", F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm"))
              .withColumn("event_day", F.date_format("cancelled_ts", "yyyy-MM-dd"))
              .withColumn("ingested_at", F.current_timestamp()))
    write_hudi(cancel, hudi_opts("cancellations", "bronze",
                                 pk="cancellation_id", partition_field="event_day",
                                 precombine="ingested_at"))

    rates = (batch_df.filter(F.col("_source") == "exchange_rate")
             .withColumn("ingested_at", F.current_timestamp()))
    write_hudi(rates, hudi_opts("exchange_rates", "bronze",
                                pk="update_id", partition_field="",
                                precombine="timestamp"))

    print(f"[batch {batch_id}] tx={tx.count()} cancel={cancel.count()} rates={rates.count()}")
    batch_df.unpersist()


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "kafka.npl.svc.cluster.local:9092"
    topic = sys.argv[2] if len(sys.argv) > 2 else "lab08_transactions"
    starting = sys.argv[3] if len(sys.argv) > 3 else "earliest"

    spark = (SparkSession.builder
             .appName("bronze-kafka-ingest")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", starting)
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts", "offset AS kafka_offset")
              .select(F.from_json("json", EVENT_SCHEMA).alias("e"), "kafka_ts", "kafka_offset")
              .select("e.*", "kafka_ts", "kafka_offset"))

    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "s3a://hudi/.checkpoints/bronze-kafka")
             .trigger(processingTime="60 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()

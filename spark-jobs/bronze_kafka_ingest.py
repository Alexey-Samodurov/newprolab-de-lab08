import sys
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from hudi_utils import hudi_opts, write_hudi
from watermark_utils import (
    bootstrap_watermark_table,
    extract_source_partitions_from_column,
    write_watermark,
)


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


def process_batch(spark, batch_df, batch_id):
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
    tx.persist()
    tx_count = tx.count()
    if tx_count > 0:
        write_hudi(tx, hudi_opts(
            "transactions", "bronze",
            pk="composite_pk", partition_field="event_day",
            precombine="ingested_at", table_suffix="_kafka",
            column_stats_cols="event_day,status,transaction_type,ingested_at",
        ))
        # Watermark пишется по event_day (event-time), не по дате kafka_ts:
        # DAG-сенсор фильтрует по event_day=ds в bronze.transactions, поэтому
        # ключ watermark должен совпадать с тем, что dbt видит в данных.
        write_watermark(
            spark, "transactions",
            extract_source_partitions_from_column(tx, "event_day"),
            tx_count, batch_id,
        )

    cancel = (batch_df.filter(F.col("_source") == "cancellation")
              .withColumn("cancelled_ts", F.to_timestamp("cancelled_at", "yyyy MMM dd HH:mm"))
              .withColumn("event_day", F.date_format("cancelled_ts", "yyyy-MM-dd"))
              .withColumn("ingested_at", F.current_timestamp()))
    cancel.persist()
    cancel_count = cancel.count()
    if cancel_count > 0:
        write_hudi(cancel, hudi_opts(
            "cancellations", "bronze",
            pk="cancellation_id", partition_field="event_day",
            precombine="ingested_at", table_suffix="_kafka",
            column_stats_cols="event_day,reason,ingested_at",
        ))
        write_watermark(
            spark, "cancellations",
            extract_source_partitions_from_column(cancel, "event_day"),
            cancel_count, batch_id,
        )

    rates = (batch_df.filter(F.col("_source") == "exchange_rate")
             .withColumn("ingested_at", F.current_timestamp()))
    rates.persist()
    rates_count = rates.count()
    if rates_count > 0:
        write_hudi(rates, hudi_opts(
            "exchange_rates", "bronze",
            pk="update_id", partition_field="",
            precombine="timestamp", table_suffix="_kafka",
            column_stats_cols="timestamp",
            enable_record_index=False,
        ))
        write_watermark(
            spark, "exchange_rates",
            ["__nonpartitioned__"],
            rates_count, batch_id,
        )

    print(f"[batch {batch_id}] tx={tx_count} cancel={cancel_count} rates={rates_count}")
    tx.unpersist()
    cancel.unpersist()
    rates.unpersist()
    batch_df.unpersist()


def main():
    bootstrap = (sys.argv[1] if len(sys.argv) > 1
                 else os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
    topic = (sys.argv[2] if len(sys.argv) > 2
             else os.environ.get("KAFKA_TOPIC", "lab08_transactions"))
    starting = sys.argv[3] if len(sys.argv) > 3 else "earliest"

    if not bootstrap:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS не задан (ни через argv, ни через env). "
            "Проверь Secret lab08-credentials и envSecretKeyRefs в SparkApplication.")

    spark = (SparkSession.builder
             .appName("bronze-kafka-ingest")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    bootstrap_watermark_table(spark)

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

    from functools import partial
    query = (parsed.writeStream
             .foreachBatch(partial(process_batch, spark))
             .option("checkpointLocation", "s3a://checkpoints/bronze-kafka")
             .trigger(processingTime="60 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()

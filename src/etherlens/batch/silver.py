"""Silver job — cleans bronze Parquet into silver/tx partitioned by dt."""
from __future__ import annotations

import json
import os
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, TimestampType,
)
from pyspark.storagelevel import StorageLevel

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark


def run(env: str, bucket: str | None = None, bronze_path: str | None = None,
        spark: SparkSession | None = None) -> dict:
    """Run the silver job. Returns stats dict."""
    t0 = time.time()
    paths = get_paths(env, bucket)
    if bronze_path:
        paths["bronze"] = bronze_path

    _spark = spark or get_spark(env, "etherlens-silver")

    # 1. Read bronze; cast every column explicitly
    bronze = (
        _spark.read.parquet(paths["bronze"])
        .select(
            F.col("hash").cast(StringType()),
            F.col("block_number").cast(LongType()),
            F.col("block_timestamp").cast(TimestampType()),
            F.col("from_address").cast(StringType()),
            F.col("to_address").cast(StringType()),
            F.col("value").cast(StringType()),
            F.col("gas").cast(LongType()),
            F.col("gas_price").cast(LongType()),
            F.col("nonce").cast(LongType()),
            F.col("receipt_gas_used").cast(LongType()),
            F.col("receipt_effective_gas_price").cast(LongType()),
            F.col("receipt_status").cast(LongType()),
            F.col("transaction_type").cast(LongType()),
        )
    )

    rows_in = bronze.count()

    # Measure bronze bytes
    bronze_bytes = io_utils.size_bytes(_spark, paths["bronze"])

    # 2. Accumulators — count bad rows and contract creations
    sc = _spark.sparkContext
    acc_bad_rows = sc.accumulator(0)
    acc_contract_creations = sc.accumulator(0)

    _spark.sparkContext.setJobDescription("accumulator scan")

    def _scan(row):
        if row["hash"] is None or row["from_address"] is None or row["block_timestamp"] is None:
            acc_bad_rows.add(1)
        if row["to_address"] is None:
            acc_contract_creations.add(1)

    bronze.rdd.foreach(_scan)

    acc_bad = acc_bad_rows.value
    acc_cc = acc_contract_creations.value

    # Cross-check with DataFrame aggregates
    df_bad = bronze.filter(
        F.col("hash").isNull() | F.col("from_address").isNull() | F.col("block_timestamp").isNull()
    ).count()
    df_cc = bronze.filter(F.col("to_address").isNull()).count()

    assert acc_bad == df_bad, f"Accumulator bad_rows {acc_bad} != DataFrame {df_bad}"
    assert acc_cc == df_cc, f"Accumulator contract_creations {acc_cc} != DataFrame {df_cc}"

    # 3. Drop bad rows, deduplicate
    _spark.sparkContext.setJobDescription("drop bad rows + dedup")
    clean = bronze.filter(
        F.col("hash").isNotNull() &
        F.col("from_address").isNotNull() &
        F.col("block_timestamp").isNotNull()
    )
    rows_after_bad = clean.count()
    bad_rows_removed = rows_in - rows_after_bad

    before_dedup = rows_after_bad
    # Cache the deduplicated rows: every later step reuses them, and without
    # the cache Spark would redo the dedup shuffle once per action.
    clean = clean.dropDuplicates(["hash"]).persist(StorageLevel.MEMORY_AND_DISK)
    after_dedup = clean.count()
    duplicates_removed = before_dedup - after_dedup

    # 4. Remove failed transactions
    _spark.sparkContext.setJobDescription("remove failed tx")
    before_fail = after_dedup
    clean = clean.filter(F.col("receipt_status") == 1)
    after_fail = clean.count()
    failed_removed = before_fail - after_fail

    # 5. Derive silver columns
    _spark.sparkContext.setJobDescription("derive silver columns")
    silver = clean.select(
        F.col("hash"),
        F.col("block_number"),
        F.col("block_timestamp").alias("ts"),
        F.date_format(F.col("block_timestamp"), "yyyy-MM-dd").alias("dt"),
        F.hour(F.col("block_timestamp")).cast("int").alias("hour"),
        F.dayofweek(F.col("block_timestamp")).cast("int").alias("dow"),
        F.lower(F.col("from_address")).alias("from_address"),
        F.lower(F.col("to_address")).alias("to_address"),
        F.col("to_address").isNull().alias("is_contract_creation"),
        (F.col("value").cast(DoubleType()) / 1e18).alias("value_eth"),
        F.col("gas").alias("gas_limit"),
        F.col("receipt_gas_used").alias("gas_used"),
        (F.coalesce(F.col("receipt_effective_gas_price"), F.col("gas_price")).cast(DoubleType()) / 1e9)
            .alias("gas_price_gwei"),
        (F.col("receipt_gas_used") *
         F.coalesce(F.col("receipt_effective_gas_price"), F.col("gas_price")).cast(DoubleType()) / 1e18)
            .alias("fee_eth"),
        F.col("nonce"),
        F.col("transaction_type").alias("tx_type"),
        (F.col("receipt_gas_used") == 21000).alias("is_plain_transfer"),
    )

    # 6. Write silver partitioned by dt
    _spark.sparkContext.setJobDescription("write silver")
    # repartition("dt") puts each day in one task -> one Parquet file per day.
    # Without it every day gets 200 small files (one per shuffle partition),
    # and each file costs a slow rename on Cloud Storage.
    silver.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(paths["silver"])

    # These reuse the cached rows, so no re-read of bronze and no second dedup
    rows_out = silver.count()
    days = silver.select("dt").distinct().count()

    # 7. Write edge list for MapReduce
    _spark.sparkContext.setJobDescription("write edge list")
    edges_df = silver.filter(F.col("to_address").isNotNull()).select(
        F.col("from_address"),
        F.col("to_address"),
        F.col("value_eth"),
    )
    edges_path = paths["edges"]
    io_utils.makedirs(edges_path)
    # coalesce (no shuffle) to at most 32 files: on real data each is roughly one
    # 128 MB HDFS block, so MapReduce runs 32 map tasks instead of 200 tiny ones.
    edges_df.coalesce(32).write.mode("overwrite").csv(edges_path, header=False)

    edge_count = edges_df.count()
    clean.unpersist()

    # 8. Write stats
    stats = {
        "rows_in": rows_in,
        "bad_rows": bad_rows_removed,
        "duplicates_removed": duplicates_removed,
        "failed_removed": failed_removed,
        "contract_creations": acc_cc,
        "rows_out": rows_out,
        "days": days,
        "runtime_s": round(time.time() - t0, 2),
        "bronze_bytes": bronze_bytes,
        "edge_count": edge_count,
    }
    io_utils.write_json(_spark, os.path.join(paths["results"], "silver_stats.json"), stats)

    print(f"Silver: {rows_in} in → {rows_out} out  "
          f"(bad={bad_rows_removed}, dup={duplicates_removed}, failed={failed_removed}, "
          f"cc={acc_cc}, days={days})")
    return stats


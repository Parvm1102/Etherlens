"""Gold aggregates — Phase 4.1."""
from __future__ import annotations

import json
import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark

MIN_TX_LOCAL = 3
MIN_TX_GCP = 5


def run(env: str, bucket: str | None = None, silver_path: str | None = None,
        spark: SparkSession | None = None) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    if silver_path:
        paths["silver"] = silver_path

    _spark = spark or get_spark(env, "etherlens-gold")
    min_tx = MIN_TX_LOCAL if env == "local" else MIN_TX_GCP

    _spark.sparkContext.setJobDescription("read silver")
    silver = _spark.read.parquet(paths["silver"])
    silver = silver.persist(StorageLevel.MEMORY_AND_DISK)

    gold = paths["gold"]
    _mk(gold)

    # --- daily_stats ---
    _spark.sparkContext.setJobDescription("daily_stats")
    daily_stats = silver.groupBy("dt").agg(
        F.count("*").alias("tx_count"),
        F.countDistinct("from_address").alias("active_senders"),
        F.countDistinct(
            F.when(F.col("to_address").isNotNull(), F.col("to_address"))
        ).alias("active_addresses_to"),
        F.sum("value_eth").alias("total_eth"),
        F.avg("gas_price_gwei").alias("avg_gas_gwei"),
        F.expr("percentile_approx(gas_price_gwei, 0.5)").alias("median_gas_gwei"),
        F.sum("fee_eth").alias("total_fee_eth"),
        F.avg(F.col("is_plain_transfer").cast("double")).alias("plain_transfer_share"),
        F.sum(F.col("is_contract_creation").cast("long")).alias("contract_creations"),
    ).withColumn(
        "active_addresses",
        F.col("active_senders") + F.col("active_addresses_to"),
    ).drop("active_addresses_to")
    daily_stats.write.mode("overwrite").parquet(os.path.join(gold, "daily_stats"))

    # --- hourly_gas ---
    _spark.sparkContext.setJobDescription("hourly_gas")
    hourly_gas = silver.groupBy("dt", "hour").agg(
        F.count("*").alias("tx_count"),
        F.avg("gas_price_gwei").alias("avg_gas_gwei"),
        F.expr("percentile_approx(gas_price_gwei, 0.5)").alias("median_gas_gwei"),
    )
    hourly_gas.write.mode("overwrite").parquet(os.path.join(gold, "hourly_gas"))

    # --- hour_of_day_profile ---
    _spark.sparkContext.setJobDescription("hour_of_day_profile")
    hodp2 = hourly_gas.groupBy("hour").agg(
        F.expr("percentile_approx(median_gas_gwei, 0.5)").alias("median_gas_gwei"),
        F.avg("tx_count").alias("avg_tx_count"),
    )
    hodp2.write.mode("overwrite").parquet(os.path.join(gold, "hour_of_day_profile"))

    # --- top_senders ---
    _spark.sparkContext.setJobDescription("top_senders")
    top_senders = (
        silver.groupBy("from_address").agg(
            F.count("*").alias("tx_count"),
            F.sum("value_eth").alias("total_eth_sent"),
            F.countDistinct("to_address").alias("distinct_receivers"),
            F.max("nonce").alias("max_nonce"),
        )
        .orderBy(F.desc("tx_count"))
        .limit(1000)
        .withColumnRenamed("from_address", "address")
    )
    top_senders.write.mode("overwrite").parquet(os.path.join(gold, "top_senders"))

    # --- whales ---
    _spark.sparkContext.setJobDescription("whales")
    whales = (
        silver.groupBy("from_address").agg(
            F.sum("value_eth").alias("total_eth_sent"),
            F.count("*").alias("tx_count"),
        )
        .orderBy(F.desc("total_eth_sent"))
        .limit(100)
        .withColumnRenamed("from_address", "address")
    )
    whales.write.mode("overwrite").parquet(os.path.join(gold, "whales"))

    # --- address_features ---
    _spark.sparkContext.setJobDescription("address_features")
    from_side = silver.groupBy("from_address").agg(
        F.count("*").alias("tx_out"),
        F.sum("value_eth").alias("eth_out"),
        F.max("nonce").alias("max_nonce"),
        F.avg("gas_price_gwei").alias("avg_gas_gwei"),
        F.avg(F.col("is_plain_transfer").cast("double")).alias("plain_share"),
        F.countDistinct("dt").alias("active_days"),
        F.countDistinct("hour").alias("active_hours"),
        F.countDistinct("to_address").alias("dist_rcv"),
    ).withColumnRenamed("from_address", "address")

    to_side = silver.filter(F.col("to_address").isNotNull()).groupBy("to_address").agg(
        F.count("*").alias("tx_in"),
        F.sum("value_eth").alias("eth_in"),
        F.countDistinct("from_address").alias("dist_snd"),
    ).withColumnRenamed("to_address", "address")

    addr_feat = (
        from_side.join(to_side, "address", "left")
        .fillna(0, subset=["tx_in", "eth_in", "dist_snd"])
        .withColumn("distinct_counterparties", F.col("dist_rcv") + F.col("dist_snd"))
        .filter((F.col("tx_out") + F.col("tx_in")) >= min_tx)
        .withColumn("is_central", F.lit(False))
        .select(
            "address", "tx_out", "tx_in", "eth_out", "eth_in",
            "distinct_counterparties", "max_nonce", "avg_gas_gwei",
            "plain_share", "active_days", "active_hours", "is_central",
        )
    )
    addr_feat.write.mode("overwrite").parquet(os.path.join(gold, "address_features"))

    # Count rows from the written Parquet (re-counting the DataFrames above
    # would recompute every aggregation)
    stats = {
        name: _spark.read.parquet(os.path.join(gold, name)).count()
        for name in ["daily_stats", "hourly_gas", "hour_of_day_profile",
                     "top_senders", "whales", "address_features"]
    }
    stats["runtime_s"] = round(time.time() - t0, 2)

    io_utils.write_json(_spark, os.path.join(paths["results"], "gold_stats.json"), stats)
    print(f"Gold: {stats}")
    silver.unpersist()
    return stats


def _mk(path: str):
    io_utils.makedirs(path)

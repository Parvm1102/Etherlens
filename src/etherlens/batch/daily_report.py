"""Daily report builder + Merkle tree — Phase 4.8."""
from __future__ import annotations

import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark


def _fmt(v) -> str:
    if v is None:
        return "None"
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(int(v))


def run(env: str, bucket: str | None = None, spark: SparkSession | None = None,
        run_merkle: bool = True) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    _spark = spark or get_spark(env, "etherlens-daily-report")

    gold = paths["gold"]
    io_utils.makedirs(paths["results"])

    # No cache here: filtering on the partition column dt makes Spark read only
    # that day's files, which is cheaper than scanning a cached copy of all days.
    _spark.sparkContext.setJobDescription("daily_report: read silver")
    silver = _spark.read.parquet(paths["silver"])

    _spark.sparkContext.setJobDescription("daily_report: read gold")
    daily_stats = _spark.read.parquet(os.path.join(gold, "daily_stats"))

    # Load flagged addresses for broadcast join
    flagged_path = os.path.join(gold, "flagged_addresses")
    flagged = None
    if io_utils.exists(_spark, os.path.join(flagged_path, "_SUCCESS")):
        flagged = _spark.read.parquet(flagged_path).select("address").distinct()

    dates = sorted(row["dt"] for row in silver.select("dt").distinct().collect())
    report_dir_base = os.path.join(gold, "daily_report")

    for dt in dates:
        _spark.sparkContext.setJobDescription(f"daily_report: {dt}")
        day_silver = silver.filter(F.col("dt") == dt)

        # summary rows from daily_stats
        ds_row = daily_stats.filter(F.col("dt") == dt).first()
        lines = []
        if ds_row:
            for key in ["tx_count", "active_senders", "active_addresses",
                        "total_eth", "avg_gas_gwei", "median_gas_gwei",
                        "plain_transfer_share", "contract_creations"]:
                v = ds_row[key] if key in ds_row.asDict() else None
                lines.append(f"{dt}|summary|{key}|{_fmt(v)}")

        # top_sender rows: top 50 senders that day. Ties are broken by address so
        # the report (and its Merkle root) does not depend on partition order.
        top_senders = (
            day_silver.groupBy("from_address")
            .count()
            .orderBy(F.desc("count"), F.asc("from_address"))
            .limit(50)
            .collect()
        )
        for row in top_senders:
            lines.append(f"{dt}|top_sender|{row['from_address']}|{row['count']}")

        # whale rows: top 20 by ETH sent that day
        whales = (
            day_silver.groupBy("from_address")
            .agg(F.sum("value_eth").alias("eth"))
            .orderBy(F.desc("eth"), F.asc("from_address"))
            .limit(20)
            .collect()
        )
        for row in whales:
            lines.append(f"{dt}|whale|{row['from_address']}|{_fmt(row['eth'])}")

        # flagged_active rows — broadcast join
        if flagged is not None:
            flagged_active = (
                day_silver
                .join(F.broadcast(flagged.withColumnRenamed("address", "from_address")),
                      "from_address", "inner")
                .groupBy("from_address")
                .count()
                .collect()
            )
            for row in flagged_active:
                lines.append(f"{dt}|flagged_active|{row['from_address']}|{row['count']}")

        # Sort and write
        lines = sorted(lines)
        report_path = os.path.join(report_dir_base, f"dt={dt}", "report.txt")
        io_utils.write_text(_spark, report_path, "\n".join(lines) + "\n")

    # Run Merkle CLI. On GCP the cluster has no eth_utils, so the Merkle step
    # runs locally after pulling gold/daily_report (see docs/PROGRESS.md).
    if run_merkle and env == "local":
        import subprocess
        subprocess.run(
            ["python", "-m", "etherlens.chain.merkle", "build", "--all", "--env", env],
            check=True
        )

    elapsed = round(time.time() - t0, 2)
    print(f"Daily reports written for {len(dates)} days in {elapsed}s")
    return {"dates": dates, "runtime_s": elapsed}

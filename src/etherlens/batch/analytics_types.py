"""Four analytics types — Phase 4.7, notes §1.9."""
from __future__ import annotations

import os
import time

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, Window
from pyspark.storagelevel import StorageLevel

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark


def run(env: str, bucket: str | None = None, spark: SparkSession | None = None) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    _spark = spark or get_spark(env, "etherlens-analytics")

    gold = paths["gold"]
    results_dir = paths["results"]
    io_utils.makedirs(results_dir)

    _spark.sparkContext.setJobDescription("analytics: read gold")
    daily = _spark.read.parquet(os.path.join(gold, "daily_stats"))
    hourly = _spark.read.parquet(os.path.join(gold, "hourly_gas"))
    hodp = _spark.read.parquet(os.path.join(gold, "hour_of_day_profile"))

    # ---- Descriptive ----
    _spark.sparkContext.setJobDescription("analytics: descriptive")
    agg = daily.agg(
        F.sum("tx_count").alias("total_tx"),
        F.avg("tx_count").alias("avg_tx_per_day"),
        F.sum("total_eth").alias("total_eth"),
        F.avg("avg_gas_gwei").alias("avg_gas_gwei"),
        F.expr("percentile_approx(median_gas_gwei, 0.5)").alias("median_gas_gwei"),
        F.avg("plain_transfer_share").alias("plain_transfer_share"),
    ).first()

    descriptive = {
        "total_tx": int(agg["total_tx"] or 0),
        "avg_tx_per_day": round(float(agg["avg_tx_per_day"] or 0), 1),
        "total_eth": round(float(agg["total_eth"] or 0), 2),
        "avg_gas_gwei": round(float(agg["avg_gas_gwei"] or 0), 3),
        "median_gas_gwei": round(float(agg["median_gas_gwei"] or 0), 3),
        "plain_transfer_share": round(float(agg["plain_transfer_share"] or 0), 4),
    }

    # ---- Diagnostic ----
    _spark.sparkContext.setJobDescription("analytics: diagnostic")
    corr = hourly.stat.corr("tx_count", "avg_gas_gwei")
    diagnostic = {
        "hourly_tx_gas_corr": round(corr, 4),
        "interpretation": (
            "Positive: busier hours have higher gas prices (demand drives price)."
            if corr > 0 else
            "Negative: unexpectedly lower gas at peak hours."
        ),
    }

    # ---- Prescriptive ----
    _spark.sparkContext.setJobDescription("analytics: prescriptive")
    cheapest_row = hodp.orderBy("median_gas_gwei").first()
    peak_row = hodp.orderBy(F.desc("median_gas_gwei")).first()
    cheapest_hour = int(cheapest_row["hour"])
    cheapest_gas = float(cheapest_row["median_gas_gwei"])
    peak_gas = float(peak_row["median_gas_gwei"])
    overall_med = float(hodp.agg(F.avg("median_gas_gwei")).first()[0])
    savings_vs_peak = round((peak_gas - cheapest_gas) / peak_gas * 100, 1)
    savings_vs_median = round((overall_med - cheapest_gas) / overall_med * 100, 1)

    prescriptive = {
        "cheapest_hour_utc": cheapest_hour,
        "cheapest_gas_gwei": round(cheapest_gas, 3),
        "peak_gas_gwei": round(peak_gas, 3),
        "savings_vs_peak_pct": savings_vs_peak,
        "savings_vs_median_pct": savings_vs_median,
        "recommendation": (
            f"Submit transactions at {cheapest_hour:02d}:00 UTC to save ~{savings_vs_peak}% "
            f"vs peak hour ({peak_row['hour']:02d}:00 UTC)."
        ),
    }

    # ---- Predictive (P1) ----
    _spark.sparkContext.setJobDescription("analytics: predictive")
    predictive = _run_predictive(_spark, hourly, results_dir)

    result = {
        "descriptive": descriptive,
        "diagnostic": diagnostic,
        "predictive": predictive,
        "prescriptive": prescriptive,
        "runtime_s": round(time.time() - t0, 2),
    }

    io_utils.write_json(_spark, os.path.join(results_dir, "analytics_types.json"), result)

    print(f"Analytics: cheapest_hour={cheapest_hour}, corr={corr:.4f}")
    return result


def _run_predictive(spark, hourly, results_dir: str) -> dict:
    try:
        w = Window.partitionBy().orderBy("dt", "hour")
        df = hourly.withColumn(
            "next_gas", F.lead("avg_gas_gwei").over(w)
        ).withColumn(
            "dow", F.dayofweek(F.col("dt").cast("date")).cast("int")
        ).dropna(subset=["next_gas"])

        # One-hot hour (drop one)
        for h in range(1, 24):
            df = df.withColumn(f"h_{h}", (F.col("hour") == h).cast("double"))
        hour_cols = [f"h_{h}" for h in range(1, 24)]

        feature_cols = ["avg_gas_gwei", "tx_count"] + hour_cols
        df = VectorAssembler(inputCols=feature_cols, outputCol="features",
                             handleInvalid="skip").transform(df)
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        n = df.count()
        split_idx = int(n * 0.8)
        # Chronological 80/20 split — add row number
        indexed = df.withColumn("rn", F.row_number().over(Window.partitionBy().orderBy("dt", "hour")))
        train = indexed.filter(F.col("rn") <= split_idx)
        test = indexed.filter(F.col("rn") > split_idx)

        lr = LinearRegression(featuresCol="features", labelCol="next_gas")
        model = lr.fit(train)
        preds = model.transform(test)

        from pyspark.ml.evaluation import RegressionEvaluator
        rmse = RegressionEvaluator(labelCol="next_gas", metricName="rmse").evaluate(preds)
        mae = RegressionEvaluator(labelCol="next_gas", metricName="mae").evaluate(preds)
        r2 = RegressionEvaluator(labelCol="next_gas", metricName="r2").evaluate(preds)

        # Persistence baseline: predict next = current
        base_rmse = float(preds.select(
            F.sqrt(F.avg((F.col("next_gas") - F.col("avg_gas_gwei")) ** 2))
        ).first()[0])

        df.unpersist()
        return {
            "rmse": round(rmse, 4), "mae": round(mae, 4), "r2": round(r2, 4),
            "baseline_rmse": round(base_rmse, 4),
            "beats_baseline": rmse < base_rmse,
            "note": "Chronological split (no leakage). Next-hour gas prediction.",
        }
    except Exception as e:
        return {"error": str(e), "note": "Predictive model failed gracefully"}

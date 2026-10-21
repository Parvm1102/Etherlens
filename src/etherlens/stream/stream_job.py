"""Structured Streaming job — Phase 5.2."""
from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import Counter

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.schemas import STREAM_SCHEMA
from etherlens.spark_session import get_spark

ALERT_Z = 3.0
MAX_WINDOWS = 60


# ---- Driver-side state (module-level, persists across micro-batches) ----
tumbling: dict = {}
sliding: dict = {}
_alerts: list = []

# Sketches
_cms = None
_hll = None
_reservoir = None
_bloom = None

# Exact baselines
_counter: Counter = Counter()
_seen_set: set = set()
_per_minute_exact: dict = {}

_state_lock = threading.Lock()


def _init_sketches(spark, flagged_path: str | None = None, manifest_path: str | None = None):
    global _cms, _hll, _reservoir, _bloom
    from etherlens.sketches.cms import CountMinSketch
    from etherlens.sketches.hll import HyperLogLog
    from etherlens.sketches.reservoir import Reservoir
    from etherlens.sketches.bloom import BloomFilter

    _cms = CountMinSketch(0.001, 0.01)
    _hll = HyperLogLog(14)
    _reservoir = Reservoir(100, seed=7)

    # Flagged addresses from KMeans (gold), else the mock bots from the manifest.
    # Read through Spark/io_utils so local and gs:// paths both work.
    flagged = []
    try:
        if flagged_path and io_utils.exists(spark, os.path.join(flagged_path, "_SUCCESS")):
            rows = spark.read.parquet(flagged_path).select("address").collect()
            flagged = [r["address"] for r in rows]
        if not flagged and manifest_path and io_utils.exists(spark, manifest_path):
            flagged = io_utils.read_json(spark, manifest_path).get("bots", [])
    except Exception as e:
        print(f"Could not load flagged addresses ({e}); Bloom filter starts empty")

    n = max(1000, 2 * len(flagged))
    _bloom = BloomFilter(n_expected=n, fp_target=0.01)
    for addr in flagged:
        if addr:
            _bloom.add(addr)


def _sink_tumbling(df, epoch_id, out_dir: str):
    global tumbling, _alerts
    if df.rdd.isEmpty():
        return
    rows = df.collect()
    with _state_lock:
        for row in rows:
            ws = str(row["window"]["start"])
            tumbling[ws] = {
                "window_start": ws,
                "window_end": str(row["window"]["end"]),
                "tx_count": int(row["tx_count"] or 0),
                "avg_gas_gwei": float(row["avg_gas_gwei"] or 0),
                "distinct_senders": int(row["distinct_senders"] or 0),
                "total_eth": float(row["total_eth"] or 0),
                "plain_share": float(row["plain_share"] or 0),
            }
        # Trim
        while len(tumbling) > MAX_WINDOWS:
            oldest = min(tumbling)
            del tumbling[oldest]

        # Anomaly detection
        completed = sorted(tumbling.keys())
        if len(completed) >= 10:
            recent = completed[-30:]
            counts = [tumbling[k]["tx_count"] for k in recent]
            gases = [tumbling[k]["avg_gas_gwei"] for k in recent]
            last = completed[-1]
            last_row = tumbling[last]
            for metric, vals, key in [("volume", counts, "tx_count"), ("gas", gases, "avg_gas_gwei")]:
                if len(vals) >= 2:
                    mean_ = sum(vals) / len(vals)
                    std_ = math.sqrt(sum((v - mean_) ** 2 for v in vals) / len(vals))
                    if std_ > 0:
                        z = (last_row[key] - mean_) / std_
                        if abs(z) > ALERT_Z:
                            _alerts.append({
                                "type": f"{metric}_spike",
                                "window": last,
                                "value": last_row[key],
                                "z": round(z, 3),
                            })
        _alerts = _alerts[-500:]

    spark = df.sparkSession
    _write_json(spark, os.path.join(out_dir, "latest_tumbling.json"),
                list(tumbling.values())[-20:])
    _write_json(spark, os.path.join(out_dir, "alerts.json"), _alerts)


def _sink_sliding(df, epoch_id, out_dir: str):
    global sliding
    if df.rdd.isEmpty():
        return
    rows = df.collect()
    with _state_lock:
        for row in rows:
            ws = str(row["window"]["start"])
            sliding[ws] = {
                "window_start": ws,
                "tx_count": int(row["tx_count"] or 0),
                "avg_gas_gwei": float(row["avg_gas_gwei"] or 0),
            }
        while len(sliding) > MAX_WINDOWS:
            del sliding[min(sliding)]
    _write_json(df.sparkSession, os.path.join(out_dir, "latest_sliding.json"),
                list(sliding.values())[-20:])


def _sink_sketches(df, epoch_id, out_dir: str):
    if df.rdd.isEmpty():
        return
    rows = df.collect()
    with _state_lock:
        for row in rows:
            # pyspark Row has no .get(); indexing returns None for null fields
            sender = row["from_address"] or ""
            tx_hash = row["hash"] or ""
            to = row["to_address"] or ""
            if sender:
                _cms.add(sender)
                _hll.add(sender)
                _reservoir.add(sender)
                _counter[sender] += 1
                _seen_set.add(sender)
                # Exact distinct senders per minute, to check Spark's HLL
                # (approx_count_distinct in the tumbling window) against the truth
                if row["event_time"] is not None:
                    minute = str(row["event_time"].replace(second=0, microsecond=0))
                    _per_minute_exact.setdefault(minute, set()).add(sender)

                # Bloom alert for flagged addresses
                if sender in _bloom:
                    _alerts.append({
                        "type": "flagged_address",
                        "address": sender,
                        "hash": tx_hash,
                        "note": "Bloom says maybe-in-set",
                    })
            if to and to in _bloom:
                _alerts.append({
                    "type": "flagged_address",
                    "address": to,
                    "hash": tx_hash,
                    "note": "Bloom says maybe-in-set (to_address)",
                })

        top50 = _cms.top_k(50)
        top50_out = [
            {"address": a, "estimate": e, "exact": _counter[a]}
            for a, e in top50
        ]
        top10_out = top50_out[:10]
        max_err = max((abs(e - _counter[a]) for a, e in top50), default=0)
        while len(_per_minute_exact) > MAX_WINDOWS:
            del _per_minute_exact[min(_per_minute_exact)]
        within = sum(
            1 for a, c in _counter.items()
            if _cms.estimate(a) - c <= _cms.error_bound
        )
        total_items = len(_counter)
        frac_within = within / total_items if total_items > 0 else 1.0

        sketch_metrics = {
            "n_items": _cms.N,
            "per_minute_exact_distinct": {m: len(v) for m, v in sorted(_per_minute_exact.items())},
            "cms": {
                "eps": _cms.eps, "delta": _cms.delta,
                "w": _cms.w, "d": _cms.d,
                "memory_bytes": _cms.memory_bytes,
                "error_bound": _cms.error_bound,
                "top10": top10_out,
                "top50": top50_out,
                "max_abs_error": max_err,
                "fraction_within_bound": round(frac_within, 4),
            },
            "hll": {
                "estimate": round(_hll.estimate()),
                "exact": len(_seen_set),
                "rel_error": round(abs(_hll.estimate() - len(_seen_set)) / max(1, len(_seen_set)), 4),
                "standard_error": round(_hll.standard_error, 4),
            },
            "bloom": {
                "m": _bloom.m, "k": _bloom.k,
                "n_inserted": _bloom._n,
                "theoretical_fp": round(_bloom.theoretical_fp(_bloom._n), 6),
            },
            "reservoir": {
                "s": _reservoir.s,
                "seen": _reservoir._i,
                "sample": _reservoir.sample()[:10],
            },
        }

    _write_json(df.sparkSession, os.path.join(out_dir, "sketch_metrics.json"), sketch_metrics)


def _write_json(spark, path: str, data):
    # Local: atomic temp-file + rename. GCS: objects appear atomically and are
    # rewritten whole (Cloud Storage has no append).
    io_utils.makedirs(os.path.dirname(path))
    io_utils.write_json(spark, path, data)


def run(env: str, bucket: str | None = None, max_minutes: float | None = None,
        spark: SparkSession | None = None):
    paths = get_paths(env, bucket)
    _spark = spark or get_spark(env, "etherlens-stream")
    # Micro-batches are small: 8 state partitions instead of 200 keeps each batch
    # well inside the 5 s trigger. (Fixed per checkpoint, so set before start.)
    _spark.conf.set("spark.sql.shuffle.partitions", "8")

    _init_sketches(
        _spark,
        flagged_path=os.path.join(paths["gold"], "flagged_addresses"),
        manifest_path=paths["manifest"],
    )

    stream_in = paths["stream_in"]
    out_dir = paths["stream_out"]
    ckpt_base = paths["stream_ckpt"]

    raw = (
        _spark.readStream
        .schema(STREAM_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(stream_in)
    )

    raw = (
        raw
        .withColumn("event_time", F.to_timestamp("block_timestamp"))
        .withColumn("gas_price_gwei", F.col("gas_price_wei") / 1e9)
        .withColumn("value_eth", F.col("value_wei").cast("double") / 1e18)
        .withColumn("is_plain", F.col("gas_limit") == 21000)
    )

    # Q1 tumbling 1-minute window
    q1 = (
        raw.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"))
        .agg(
            F.count("*").alias("tx_count"),
            F.avg("gas_price_gwei").alias("avg_gas_gwei"),
            F.approx_count_distinct("from_address").alias("distinct_senders"),
            F.sum("value_eth").alias("total_eth"),
            F.avg(F.col("is_plain").cast("double")).alias("plain_share"),
        )
    )

    # Q2 sliding 5-min / 1-min slide
    q2 = (
        raw.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "5 minutes", "1 minute"))
        .agg(
            F.count("*").alias("tx_count"),
            F.avg("gas_price_gwei").alias("avg_gas_gwei"),
        )
    )

    # Q3 raw → sketches
    q3 = raw.select("hash", "from_address", "to_address", "value_eth", "event_time")

    def sink_t(df, eid): _sink_tumbling(df, eid, out_dir)
    def sink_s(df, eid): _sink_sliding(df, eid, out_dir)
    def sink_sk(df, eid): _sink_sketches(df, eid, out_dir)

    sq1 = (q1.writeStream.outputMode("update")
           .foreachBatch(sink_t)
           .option("checkpointLocation", os.path.join(ckpt_base, "tumbling"))
           .trigger(processingTime="5 seconds")
           .start())

    sq2 = (q2.writeStream.outputMode("update")
           .foreachBatch(sink_s)
           .option("checkpointLocation", os.path.join(ckpt_base, "sliding"))
           .trigger(processingTime="5 seconds")
           .start())

    sq3 = (q3.writeStream.outputMode("append")
           .foreachBatch(sink_sk)
           .option("checkpointLocation", os.path.join(ckpt_base, "sketches"))
           .trigger(processingTime="5 seconds")
           .start())

    if max_minutes:
        time.sleep(max_minutes * 60)
        sq1.stop(); sq2.stop(); sq3.stop()
    else:
        _spark.streams.awaitAnyTermination()

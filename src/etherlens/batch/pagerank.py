"""PageRank on RDDs — Phase 4.5, notes §5.7, verbatim class algorithm."""
from __future__ import annotations

import os
import time
from operator import add

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark

PAGERANK_DAYS = 7
ITERS = 10


def run(env: str, bucket: str | None = None, silver_path: str | None = None,
        spark: SparkSession | None = None) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    if silver_path:
        paths["silver"] = silver_path

    _spark = spark or get_spark(env, "etherlens-pagerank")
    sc = _spark.sparkContext

    # Checkpoint dir for lineage truncation
    ckpt_dir = paths["rdd_ckpt"]
    io_utils.makedirs(ckpt_dir)
    sc.setCheckpointDir(ckpt_dir)

    # Read silver and filter to last PAGERANK_DAYS
    _spark.sparkContext.setJobDescription("pagerank: read silver")
    silver = _spark.read.parquet(paths["silver"])
    from datetime import date, timedelta
    max_dt_row = silver.agg(F.max("dt")).first()
    max_dt = str(max_dt_row[0]) if max_dt_row and max_dt_row[0] else None
    if max_dt is None:
        raise RuntimeError("Silver data is empty — run make silver first")
    cutoff = (date.fromisoformat(max_dt) - timedelta(days=PAGERANK_DAYS - 1)).isoformat()

    pairs = (
        silver.filter((F.col("dt") >= cutoff) & F.col("to_address").isNotNull())
        .select("from_address", "to_address")
        .rdd.map(tuple)
    )

    # Verbatim class algorithm
    _spark.sparkContext.setJobDescription("pagerank: build links")
    links = pairs.distinct().groupByKey().mapValues(list).cache()
    ranks = links.mapValues(lambda _: 1.0)

    iter_times = []
    for i in range(ITERS):
        _spark.sparkContext.setJobDescription(f"pagerank: iteration {i+1}")
        t_iter = time.time()
        contribs = links.join(ranks).flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]
        )
        ranks = contribs.reduceByKey(add).mapValues(lambda r: 0.15 + 0.85 * r)
        if (i + 1) % 5 == 0:
            ranks.checkpoint()
            ranks = ranks.persist(StorageLevel.MEMORY_AND_DISK)
        iter_times.append(round(time.time() - t_iter, 3))

    # Collect top 1000
    _spark.sparkContext.setJobDescription("pagerank: collect top 1000")
    top1000 = ranks.top(1000, key=lambda kv: kv[1])

    # Write gold/pagerank as Parquet
    gold_pr_path = os.path.join(paths["gold"], "pagerank")
    io_utils.makedirs(os.path.dirname(gold_pr_path))
    pr_df = _spark.createDataFrame(top1000, ["address", "rank"])
    pr_df.write.mode("overwrite").parquet(gold_pr_path)

    # Write results/pagerank_top.csv
    results_dir = paths["results"]
    top50 = top1000[:50]
    csv_text = "address,rank\n" + "".join(f"{addr},{rank:.6f}\n" for addr, rank in top50)
    io_utils.write_text(_spark, os.path.join(results_dir, "pagerank_top.csv"), csv_text)

    # Write top-100 central addresses to a separate small file (avoids overwriting address_features)
    top100_addrs = [addr for addr, _ in top1000[:100]]
    central_df = _spark.createDataFrame([(a,) for a in top100_addrs], ["address"])
    central_path = os.path.join(paths["gold"], "central_addresses")
    central_df.write.mode("overwrite").parquet(central_path)

    stats = {
        "iters": ITERS,
        "iter_times_s": iter_times,
        "top_address": top1000[0][0] if top1000 else None,
        "top_rank": top1000[0][1] if top1000 else None,
        "runtime_s": round(time.time() - t0, 2),
    }

    io_utils.write_json(_spark, os.path.join(results_dir, "pagerank_iters.json"), stats)

    print(f"PageRank top: {top1000[0] if top1000 else 'none'}")
    links.unpersist()
    return stats

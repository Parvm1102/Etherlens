"""Count per sender using Spark RDDs — Phase 4.4, notes §5.4."""
from __future__ import annotations

import os
import time
from operator import add

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark


def run(env: str, bucket: str | None = None, spark=None) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    _spark = spark or get_spark(env, "etherlens-spark-count")
    sc = _spark.sparkContext

    edges_path = paths["edges"]
    out_path = os.path.join(paths["results"], "mapreduce", "spark_counts")

    sc.setJobDescription("spark count per sender")
    rdd = (
        sc.textFile(edges_path)
        .map(lambda l: (l.split(",")[0], 1))
        .reduceByKey(add)
        .map(lambda kv: f"{kv[0]}\t{kv[1]}")
    )

    # saveAsTextFile fails if the output exists, so clear the previous run
    io_utils.delete(_spark, out_path)
    io_utils.makedirs(os.path.dirname(out_path))

    t_action = time.time()
    rdd.saveAsTextFile(out_path)
    elapsed = time.time() - t_action

    stats = {"spark_count_runtime_s": round(elapsed, 3), "output": out_path}
    io_utils.write_json(_spark, os.path.join(paths["results"], "spark_count_timing.json"), stats)

    print(f"Spark count: {elapsed:.2f}s → {out_path}")
    return stats

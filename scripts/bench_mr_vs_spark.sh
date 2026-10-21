#!/usr/bin/env bash
set -euo pipefail
# GCP only: benchmark MapReduce vs Spark count-per-sender 3 times each.

source .env 2>/dev/null || true

RUNS=3
declare -a MR_TIMES=()
declare -a SPARK_TIMES=()

for i in $(seq 1 $RUNS); do
    echo "=== MR run $i ==="
    START=$SECONDS
    # MR with combiner vs Spark reduceByKey: both combine map-side, so it is a fair match
    bash mapreduce/run_dataproc.sh with_combiner
    MR_TIMES+=($((SECONDS - START)))

    echo "=== Spark run $i ==="
    START=$SECONDS
    bash infra/submit.sh count_spark --env gcp --bucket "$GCS_BUCKET"
    SPARK_TIMES+=($((SECONDS - START)))
done

python3 - <<PYEOF
import json, statistics
mr = [$(IFS=,; echo "${MR_TIMES[*]}")]
sp = [$(IFS=,; echo "${SPARK_TIMES[*]}")]
result = {
    "note": "wall-clock per job submission (incl. gcloud overhead); MR = with_combiner",
    "mr_times_s": mr,
    "spark_times_s": sp,
    "mr_median_s": statistics.median(mr),
    "spark_median_s": statistics.median(sp),
}
import os
os.makedirs("results_gcp", exist_ok=True)   # GCP-only numbers live next to the pulled results
with open("results_gcp/bench_mr_vs_spark.json", "w") as f:
    json.dump(result, f, indent=2)
print(result)
PYEOF

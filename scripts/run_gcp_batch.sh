#!/usr/bin/env bash
# Phase 8: run the whole batch chain on the Dataproc cluster, in spec order,
# and record the wall-clock time of every job.
# Usage (inside the dev container):
#   bash scripts/run_gcp_batch.sh                                   # real data: gs://$GCS_BUCKET/bronze/tx
#   bash scripts/run_gcp_batch.sh gs://$GCS_BUCKET/bronze_mock/tx   # Stage B smoke test
#   SKIP="gold hive" bash scripts/run_gcp_batch.sh                  # resume, skipping steps
set -euo pipefail

source .env 2>/dev/null || true

BRONZE="${1:-gs://$GCS_BUCKET/bronze/tx}"
SKIP="${SKIP:-}"
mkdir -p logs
TIMES="logs/gcp_job_times_$(date +%Y%m%d_%H%M%S).tsv"
echo "Bronze input: $BRONZE"

run() {  # run <name> <command...>
    local name="$1"; shift
    if [[ " $SKIP " == *" $name "* ]]; then echo "=== $name (skipped) ==="; return; fi
    local start=$SECONDS
    echo "=== $name ==="
    "$@"
    echo -e "${name}\t$((SECONDS - start))" | tee -a "$TIMES"
}

run silver       bash infra/submit.sh silver --env gcp --bucket "$GCS_BUCKET" --bronze-path "$BRONZE"
run gold         bash infra/submit.sh gold --env gcp --bucket "$GCS_BUCKET"
run hive         make gcp-hive
run mapreduce    bash mapreduce/run_dataproc.sh
run spark_count  bash infra/submit.sh count_spark --env gcp --bucket "$GCS_BUCKET"
run pagerank     bash infra/submit.sh pagerank --env gcp --bucket "$GCS_BUCKET"
run kmeans       bash infra/submit.sh kmeans --env gcp --bucket "$GCS_BUCKET"
run analytics    bash infra/submit.sh analytics --env gcp --bucket "$GCS_BUCKET"
run daily_report bash infra/submit.sh daily_report --env gcp --bucket "$GCS_BUCKET"

echo "Job times (seconds) written to $TIMES"

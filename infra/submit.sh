#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

JOB="${1:?Usage: submit.sh <job_name> [args...]}"
shift

mkdir -p logs

# Executor layout for e2-standard-2 workers (8 GB RAM, 2 vCPU): one 2-core
# executor per worker. Dataproc's default (two 1-core executors with 2.9 GB heap
# each) left no room for off-heap memory, and on real data the node's earlyoom
# killed executors. Override with SPARK_PROPS=... if the cluster shape changes.
PROPS="${SPARK_PROPS:-spark.executor.cores=2,spark.executor.memory=3g,spark.executor.memoryOverhead=1g}"

gcloud dataproc jobs submit pyspark "gs://$GCS_BUCKET/code/jobs/${JOB}_main.py" \
    --cluster="$CLUSTER_NAME" --region="$GCP_REGION" \
    --py-files="gs://$GCS_BUCKET/code/etherlens.zip" \
    --properties="$PROPS" \
    -- "$@" 2>&1 | tee "logs/${JOB}_$(date +%Y%m%d_%H%M%S).log"

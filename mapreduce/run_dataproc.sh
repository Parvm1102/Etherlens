#!/usr/bin/env bash
set -euo pipefail
# Phase 8 only — runs inside the dev container where .env vars are loaded.
# Usage: bash mapreduce/run_dataproc.sh [no_combiner|with_combiner]   (default: both)

source .env 2>/dev/null || true

STREAMING_JAR="/usr/lib/hadoop/hadoop-streaming.jar"   # Dataproc 2.2 location
VARIANTS="${1:-no_combiner with_combiner}"

mkdir -p logs

for VARIANT in $VARIANTS; do
    OUT_DIR="/etherlens/mr_out_${VARIANT}"
    echo "=== MapReduce: ${VARIANT} ==="

    # Hadoop Streaming fails if the output directory exists -> remove it first.
    # SSH user "etherlens": the container runs as root and GCE refuses root logins.
    gcloud compute ssh "etherlens@${CLUSTER_NAME}-m" --zone="${GCP_ZONE}" --quiet \
        --command "sudo hdfs dfs -rm -r -f ${OUT_DIR}" || true

    EXTRA=()
    if [[ "$VARIANT" == "with_combiner" ]]; then
        EXTRA=(-combiner "python3 reducer.py")
    fi

    gcloud dataproc jobs submit hadoop \
        --cluster="${CLUSTER_NAME}" --region="${GCP_REGION}" \
        --jar="file://${STREAMING_JAR}" -- \
        -files "gs://${GCS_BUCKET}/code/mapreduce/mapper.py,gs://${GCS_BUCKET}/code/mapreduce/reducer.py" \
        -mapper "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        "${EXTRA[@]}" \
        -numReduceTasks 4 \
        -input /etherlens/edges_csv \
        -output "${OUT_DIR}" \
        2>&1 | tee "logs/mr_${VARIANT}_$(date +%Y%m%d_%H%M%S).log"
done

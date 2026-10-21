#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

gcloud dataproc clusters delete "$CLUSTER_NAME" \
    --region="$GCP_REGION" --quiet
echo "Cluster $CLUSTER_NAME deleted."

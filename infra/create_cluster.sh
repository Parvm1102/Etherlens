#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

# Deviations from the spec (see docs/PROGRESS.md, Phase 8):
#  --public-ip-address : image 2.2 defaults to internal IPs only, so the init
#                        action could not reach PyPI and SSH had no route.
#  pd-balanced disks   : 50 GB pd-standard gives ~6 MB/s, far too slow for shuffles.
# Shape comes from .env; defaults are the spec's free-trial shape (6 vCPUs).
# Real data (Stage C) needs more memory: see docs/PROGRESS.md, Phase 8.
MASTER_TYPE="${MASTER_TYPE:-e2-standard-2}"
WORKER_TYPE="${WORKER_TYPE:-e2-standard-2}"
NUM_WORKERS="${NUM_WORKERS:-2}"

echo "Creating Dataproc cluster: $CLUSTER_NAME in $GCP_REGION ($MASTER_TYPE + $NUM_WORKERS x $WORKER_TYPE)"
echo "This will use compute credits (~\$0.3/hour). Ctrl+C to cancel."
sleep 5

gcloud dataproc clusters create "$CLUSTER_NAME" \
    --region="$GCP_REGION" --zone="$GCP_ZONE" \
    --image-version=2.2-debian12 \
    --master-machine-type="$MASTER_TYPE" --master-boot-disk-size=50GB \
    --num-workers="$NUM_WORKERS" --worker-machine-type="$WORKER_TYPE" --worker-boot-disk-size=50GB \
    --master-boot-disk-type=pd-balanced --worker-boot-disk-type=pd-balanced \
    --public-ip-address \
    --optional-components=JUPYTER --enable-component-gateway \
    --initialization-actions="gs://$GCS_BUCKET/infra/init_actions.sh" \
    --properties="spark:spark.sql.session.timeZone=UTC" \
    --max-idle=1h

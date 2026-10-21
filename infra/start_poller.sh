#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

MODE="${1:-mock}"

gcloud compute ssh "etherlens@${CLUSTER_NAME}-m" --zone="${GCP_ZONE}" --quiet --command \
    "gcloud storage cp gs://$GCS_BUCKET/code/etherlens.zip ~/ && \
     tmux new -d -s poller \
       'ETH_RPC_URL=$ETH_RPC_URL PYTHONPATH=\$HOME/etherlens.zip \
        /opt/poller-venv/bin/python -m etherlens.stream.poller \
        --mode $MODE --out gs://$GCS_BUCKET/stream/incoming'"

echo "Poller started in tmux session 'poller' on ${CLUSTER_NAME}-m"

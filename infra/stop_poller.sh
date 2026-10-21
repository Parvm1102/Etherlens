#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

gcloud compute ssh "etherlens@${CLUSTER_NAME}-m" --zone="${GCP_ZONE}" --quiet --command \
    "tmux kill-session -t poller 2>/dev/null || echo 'No poller session found.'"

echo "Poller stopped."

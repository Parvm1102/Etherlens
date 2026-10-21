#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

gcloud config set project "$GCP_PROJECT"
gcloud services enable \
    dataproc.googleapis.com \
    compute.googleapis.com \
    storage.googleapis.com \
    bigquery.googleapis.com

gcloud storage buckets create "gs://$GCS_BUCKET" \
    --location=US \
    --uniform-bucket-level-access \
    2>/dev/null || echo "Bucket gs://$GCS_BUCKET already exists or creation skipped."

echo "Bootstrap complete."

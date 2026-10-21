#!/usr/bin/env bash
set -euo pipefail

source .env 2>/dev/null || true

# Build the zip
make package

# Upload code
gcloud storage cp dist/etherlens.zip "gs://$GCS_BUCKET/code/"
gcloud storage cp jobs/*.py "gs://$GCS_BUCKET/code/jobs/"
gcloud storage cp mapreduce/mapper.py mapreduce/reducer.py "gs://$GCS_BUCKET/code/mapreduce/"
gcloud storage cp infra/init_actions.sh "gs://$GCS_BUCKET/infra/"

# Render and upload hive scripts
mkdir -p build/hive
python scripts/render_template.py hive/create_tables.hql > build/hive/create_tables.hql
python scripts/render_template.py hive/queries.hql > build/hive/queries.hql
gcloud storage cp build/hive/*.hql "gs://$GCS_BUCKET/code/hive/"

echo "Code uploaded to gs://$GCS_BUCKET/code/"

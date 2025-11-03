#!/usr/bin/env bash
set -euo pipefail
PROJECT_ID="$1"
REGION="us-central1"
TEMPLATE="gs://${PROJECT_ID}-templates/dev/bronze_template.json"

gcloud dataflow flex-template run "bronze-$(date +%Y%m%d-%H%M%S)" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --template-file-gcs-location "${TEMPLATE}" \
  --parameters input_url="gs://${PROJECT_ID}-raw/in/input*.json",output_path="gs://${PROJECT_ID}-raw/bronze/out/prefix",staging_location="gs://${PROJECT_ID}-staging/dataflow/staging",temp_location="gs://${PROJECT_ID}-temp/dataflow/tmp"

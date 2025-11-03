#!/usr/bin/env bash
set -euo pipefail
PROJECT_ID="$1"
REGION="us-central1" # ou southamerica-east1
REPO="dataflow"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/bronze:latest"
TEMPLATE="gs://${PROJECT_ID}-templates/dev/bronze_template.json"

gcloud builds submit src/bronze \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE}"

gcloud dataflow flex-template build "${TEMPLATE}" \
  --project "${PROJECT_ID}" \
  --image "${IMAGE}" \
  --sdk-language "PYTHON" \
  --metadata-file "src/bronze/metadata.json"

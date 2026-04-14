#!/usr/bin/env bash
# Deploy Play Attribution Intelligence to GCP Cloud Run
set -euo pipefail

: "${GCP_PROJECT:?Set GCP_PROJECT environment variable}"
: "${GCP_REGION:=us-central1}"

IMAGE="gcr.io/${GCP_PROJECT}/play-attribution"

echo "Building Docker image..."
docker build -t "${IMAGE}" .

echo "Pushing to Container Registry..."
docker push "${IMAGE}"

echo "Deploying to Cloud Run..."
gcloud run deploy play-attribution \
  --image "${IMAGE}" \
  --platform managed \
  --region "${GCP_REGION}" \
  --allow-unauthenticated \
  --port 8501 \
  --memory 2Gi \
  --cpu 2 \
  --set-env-vars "PYTHONUNBUFFERED=1"

echo "Deployment complete!"
gcloud run services describe play-attribution \
  --platform managed \
  --region "${GCP_REGION}" \
  --format "value(status.url)"

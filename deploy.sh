#!/usr/bin/env bash
# Terra Metorik ETL — Deploy to dev or prod
# Usage: ./deploy.sh dev
#        ./deploy.sh prod

set -euo pipefail

ENV=${1:-}
if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "❌ Usage: ./deploy.sh dev|prod"
  exit 1
fi

if [[ "$ENV" == "prod" ]]; then
  PROJECT="terra-analytics-prod"
  JOB="terra-metorik-etl"
  SCHEDULE="0 7 * * *"
else
  PROJECT="terra-analytics-dev"
  JOB="terra-metorik-etl-dev"
  SCHEDULE="0 7 * * *"
fi

REGION="us-central1"
IMAGE="gcr.io/${PROJECT}/${JOB}:latest"
SA="terra-etl-runner@${PROJECT}.iam.gserviceaccount.com"

echo "🚀 Deploying terra-metorik-etl → ${PROJECT} [${ENV}]"

echo "🔨 Building and pushing image..."
gcloud builds submit . \
  --tag="${IMAGE}" \
  --project="${PROJECT}"

echo "📦 Creating/updating Cloud Run Job..."
gcloud run jobs update "${JOB}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --service-account="${SA}" \
  --memory=512Mi \
  --cpu=1 \
  --task-timeout=1800 \
  --max-retries=2 \
  --set-secrets="METORIK_API_KEY=metorik-api-key:latest" \
  --set-env-vars="BQ_PROJECT=${PROJECT}" \
  2>/dev/null || \
gcloud run jobs create "${JOB}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --service-account="${SA}" \
  --memory=512Mi \
  --cpu=1 \
  --task-timeout=1800 \
  --max-retries=2 \
  --set-secrets="METORIK_API_KEY=metorik-api-key:latest" \
  --set-env-vars="BQ_PROJECT=${PROJECT}"

echo "⏰ Creating/updating Cloud Scheduler..."
gcloud scheduler jobs update http "schedule-${JOB}" \
  --location="${REGION}" \
  --project="${PROJECT}" \
  --schedule="${SCHEDULE}" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --message-body="{}" \
  --oauth-service-account-email="${SA}" \
  2>/dev/null || \
gcloud scheduler jobs create http "schedule-${JOB}" \
  --location="${REGION}" \
  --project="${PROJECT}" \
  --schedule="${SCHEDULE}" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --message-body="{}" \
  --oauth-service-account-email="${SA}"

echo "✅ Done — ${JOB} scheduled daily at 7am PT → ${PROJECT}"

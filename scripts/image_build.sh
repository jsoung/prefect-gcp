#!/bin/bash
set -euo pipefail

# --- CONFIG ---
REGION="us-west1"
REPO="dev"
PROJECT_ID=$(gcloud config get-value project)

# Automatically derive image name from the project root folder
IMAGE_NAME=$(basename "$(pwd)")

# --- GIT INFO ---
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD | sed 's/[^a-zA-Z0-9_.-]/-/g')
TIMESTAMP=$(date +%s)

# --- Handle test mode ---
TEST_TS=""
if [ "${1:-}" = "test" ]; then
  TEST_TS="$TIMESTAMP"
fi

echo "🛠 Building Docker image for project: $IMAGE_NAME"
echo "   Branch: $BRANCH_NAME"
echo "   Test timestamp: ${TEST_TS:-<none>}"

# --- Submit Cloud Build ---
gcloud builds submit . \
  --region="$REGION" \
  --config=docker/cloudbuild.yaml \
  --substitutions=_REGION="$REGION",_REPO="$REPO",_IMAGE="$IMAGE_NAME",_BRANCH="$BRANCH_NAME",_TEST_TS="$TEST_TS"

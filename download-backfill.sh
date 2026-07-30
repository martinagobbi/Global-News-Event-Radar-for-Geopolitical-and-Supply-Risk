#!/bin/bash
# Downloads the backfill data from the GitHub Release and loads it into the Docker container

RELEASE_URL="https://github.com/martinagobbi/Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk/releases/download/gdelt-backfill-30d/gdelt-zip.zip"

echo "[1/4] Downloading backfill data..."
curl -L -o gdelt-zip.zip "$RELEASE_URL"

echo "[2/4] Extracting GDELT ZIP files..."
mkdir -p ./gdelt-zip
unzip -q gdelt-zip.zip -d ./gdelt-zip

echo "[3/4] Copying ZIP files into the Docker container..."
docker compose up -d ingestion

docker cp ./gdelt-zip/. pipeline_ingestion:/data/raw/zip/

echo "[4/4] Cleaning up temporary files..."
rm -rf ./gdelt-zip gdelt-zip.zip

echo "[DONE] Backfill data successfully loaded."
echo "You can now start the full pipeline with:"
echo "docker compose up"
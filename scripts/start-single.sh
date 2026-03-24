#!/usr/bin/env bash
#
# Start a single TeraSlab node in Docker.
#
# Usage:
#   ./scripts/start-single.sh                          # use ghcr.io/icellan/teraslab:latest
#   TERASLAB_IMAGE=teraslab-server:latest ./scripts/start-single.sh  # use local image
#
# Ports:
#   3300  - wire protocol (client connections)
#   9100  - HTTP (health, metrics, status)
#
# Data is persisted in a Docker volume (teraslab-single-data).
# Stop with: docker stop teraslab
# Remove:    docker rm teraslab && docker volume rm teraslab-single-data

set -euo pipefail

IMAGE="${TERASLAB_IMAGE:-ghcr.io/icellan/teraslab:latest}"
CONTAINER="teraslab"

# Stop existing container if running
docker rm -f "$CONTAINER" 2>/dev/null || true

echo "Starting TeraSlab single node..."
echo "  Image:         $IMAGE"
echo "  Wire protocol: localhost:3300"
echo "  HTTP/health:   localhost:9100"
echo ""

docker run \
    --name "$CONTAINER" \
    --rm \
    -p 3300:3300 \
    -p 9100:9100 \
    -v teraslab-single-data:/data \
    -v teraslab-single-blobstore:/blobstore \
    "$IMAGE"

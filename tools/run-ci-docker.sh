#!/bin/bash
set -euo pipefail

CI_IMAGE="golangci/golangci-lint:v2.7.2"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Using CI image: $CI_IMAGE"
echo "Workspace: $WORKSPACE_DIR"
echo ""

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not available"
    exit 1
fi

# Running CI in Docker is required to properly reach Mongo cluster addresses from within the test container
# it's required to use the host network. This is not possible to do with github actions container, so we run it in a docker container manually.
docker run --rm \
    --network host \
    -e DOCKER_HOST=unix:///var/run/docker.sock \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    -v "$WORKSPACE_DIR:/workspace" \
    -w /workspace \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --privileged \
    "$CI_IMAGE" \
    bash -c "make ci && make test-coverage"


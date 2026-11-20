#!/usr/bin/env bash
# ./build.sh <tag>
#
#   ./build.sh NF8HTTP_noanylog_Readframch0_1020v8
#     builds image  free5gc-custom-v4.2.2:NF8HTTP_noanylog_Readframch0_1020v8
#     saves it to   Build/free5gc-custom-v4.2.2-NF8HTTP_noanylog_Readframch0_1020v8.tar
#
# The build context is the repo root (the parent of this Build/ folder), because
# Dockerfile.custom does COPY . . and needs NFs/ and xnet/ under /src.
set -euo pipefail

TAG="${1:?usage: ./build.sh <tag>   e.g. ./build.sh NF8HTTP_noanylog_Readframch0_1020v8}"

BUILD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$BUILD_DIR")"
IMAGE="free5gc-custom-v4.2.2:$TAG"
TAR="$BUILD_DIR/free5gc-custom-v4.2.2-$TAG.tar"

cd "$REPO_DIR"
docker build -f "$BUILD_DIR/Dockerfile.custom" -t "$IMAGE" .
docker save "$IMAGE" -o "$TAR"

echo "image: $IMAGE"
echo "tar:   $TAR"

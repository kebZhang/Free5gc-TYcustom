#!/usr/bin/env bash
# bash build.sh <tag>
#
#   bash build.sh NF1HTTP_alllog_Readframch0_GCon_PFCPlog_1122v1
#     builds image  free5gc-custom-v4.2.2:NF1HTTP_alllog_Readframch0_GCon_PFCPlog_1122v1
#     saves it to   Build/free5gc-custom-v4.2.2-NF1HTTP_alllog_Readframch0_GCon_PFCPlog_1122v1.tar
#
# One image, one tar, 14 binaries under /free5gc/:
#   amf ausf bsf nrf nssf pcf smf udm udr chf nef n3iwf tngf  (free5gc v4.2.2 code, Go 1.25)
#   upf                                                       (go-upf 922281ba = v3.4.3, Go 1.21.8)
# [UPF in image 1122] upf was added; deploy.sh points the UPF at this image too.
#
# The build context is the repo root (the parent of this Build/ folder), because
# Dockerfile.custom does COPY . . and needs NFs/, xnet/ and pfcp/ under /src.
# Run it with bash: the script is not executable in git (mode 100644).
set -euo pipefail

TAG="${1:?usage: bash build.sh <tag>   e.g. bash build.sh NF1HTTP_alllog_Readframch0_GCon_PFCPlog_1122v1}"

BUILD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$BUILD_DIR")"
IMAGE="free5gc-custom-v4.2.2:$TAG"
TAR="$BUILD_DIR/free5gc-custom-v4.2.2-$TAG.tar"

cd "$REPO_DIR"
docker build -f "$BUILD_DIR/Dockerfile.custom" -t "$IMAGE" .
docker save "$IMAGE" -o "$TAR"

echo "image: $IMAGE"
echo "tar:   $TAR"
echo "NFs:   $(docker image inspect -f '{{index .Config.Labels "tycustom.nfs"}}' "$IMAGE")"

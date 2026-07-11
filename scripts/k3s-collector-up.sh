#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster="${RECSYS_K3D_CLUSTER:-recsys-local}"
image="openeventflow-collector:local"

docker build -f "$root/Dockerfile.collector" -t "$image" "$root"
k3d image import --mode direct -c "$cluster" "$image"
kubectl apply -k "$root/deploy/k3s"
kubectl -n recsys-data rollout status deployment/openeventflow-collector --timeout=180s


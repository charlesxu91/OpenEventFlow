#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module="$root/streaming/flink-recommendation"
namespace="${RECSYS_INFRA_NAMESPACE:-recsys-infra}"
bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka.recsys-infra.svc.cluster.local:9092}"

mvn -q -f "$module/pom.xml" package
jar="$module/target/flink-recommendation-0.1.0-SNAPSHOT.jar"
pod="$(kubectl -n "$namespace" get pod -l app=flink,component=jobmanager -o jsonpath='{.items[0].metadata.name}')"
kubectl -n "$namespace" cp "$jar" "$pod:/tmp/flink-recommendation.jar"
kubectl -n "$namespace" exec "$pod" -- flink run -d /tmp/flink-recommendation.jar \
  --bootstrap-servers "$bootstrap" \
  --input-topic recsys.client-behavior.v1 \
  --output-topic recsys.training-samples.v1 \
  --group-id openeventflow-recommendation-attribution-v1 \
  --window-hours 168 \
  --allowed-lateness-minutes 10

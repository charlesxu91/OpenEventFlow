# Flink recommendation attribution

Java 17 module that converts recommendation impressions and subsequent behavior into versioned training-sample upserts.

The core processor is independent of Flink and covers event-id deduplication, out-of-order attribution by
`request_id + impression_id + product_id`, event-time window closure, negative examples, conversion labels, refunds,
and net GMV. The Flink adapter supplies watermarks, key partitioning, event-time timers, checkpointed `ValueState` /
`MapState`, state TTL, and explicit state cleanup after finalization. This state survives checkpoints and rescaling;
RocksDB compaction-filter cleanup bounds abandoned keys that never receive an impression.

```bash
mvn test
mvn package -DskipTests
flink run target/flink-recommendation-0.1.0-SNAPSHOT.jar \
  --input /data/events.ndjson --output /data/training-samples \
  --window-hours 168 --allowed-lateness-minutes 10
```

Kafka mode:

```bash
flink run target/flink-recommendation-0.1.0-SNAPSHOT.jar \
  --bootstrap-servers kafka.recsys-infra.svc.cluster.local:9092 \
  --input-topic recsys.behavior-events.v1 \
  --output-topic recsys.training-samples.v1 \
  --group-id openeventflow-recommendation-attribution \
  --source-idle-seconds 60
```

For bounded integration tests, `--window-seconds` and `--allowed-lateness-seconds`
override the production-scale hour/minute options. Idle Kafka partitions are excluded
from the global watermark after `--source-idle-seconds`, preventing an empty partition
from indefinitely blocking attribution-window completion.

Kafka input resumes from committed offsets (or earliest when the group has no offset). Output uses the composite
attribution key as the Kafka record key and `AT_LEAST_ONCE` delivery. Consumers must upsert/deduplicate by that key;
switching to Kafka `EXACTLY_ONCE` additionally requires durable checkpoint storage and a unique transactional-id prefix.

File mode remains available for deterministic replay. For long-term training storage, consume the compactable output
topic into an upsert-capable Iceberg/Parquet sink. Configure checkpoint storage and the RocksDB state backend in the
cluster deployment; the operator state itself already uses Flink-managed keyed state.

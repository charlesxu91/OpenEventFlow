# Flink recommendation jobs

Java 17 / Apache Flink 1.20 module containing two complementary recommendation pipelines.

## Generic item attribution and realtime interest

`AttributionJob` consumes JSON `Event` records from `snowplow_enriched_events`, writes item/action training samples to `openeventflow_training_samples`, and routes events behind the watermark to `openeventflow_late_events`. Configure brokers and topics with `KAFKA_BOOTSTRAP_SERVERS`, `INPUT_TOPIC`, `TRAINING_TOPIC`, and `LATE_TOPIC`. It uses bounded-out-of-orderness event-time watermarks, checkpointing, restart defaults, TTL-backed event-id deduplication, and impression expiry.

`RealtimeInterestJob` writes event-time-windowed profiles to `openeventflow_interest_profiles` by default. Configure `FEATURE_TOPIC` and `INTEREST_WINDOW_MS`, or call `RealtimeInterestJob.run(FeatureSink)` to inject an idempotent deployment-specific feature-store/gRPC adapter.

Malformed records are routed to `openeventflow_parse_errors` (`PARSE_ERROR_TOPIC`). Kafka sinks use at-least-once delivery, so output identifiers and injected feature writes must remain idempotent.

## Aggregate recommendation attribution

`RecommendationAttributionJob` converts recommendation impressions and subsequent behavior into versioned training-sample upserts. Its core processor is independent of Flink and covers event-id deduplication, out-of-order attribution by `request_id + impression_id + product_id`, event-time window closure, negative examples, conversion labels, refunds, and net GMV.

The Flink adapter supplies watermarks, key partitioning, event-time timers, checkpointed state, state TTL, and explicit finalization cleanup. Kafka input resumes from committed offsets (or earliest when no group offset exists), while file mode supports deterministic replay.

```bash
mvn clean package

flink run -c io.openeventflow.recommendation.flink.RecommendationAttributionJob \
  target/flink-recommendation-0.1.0-SNAPSHOT.jar \
  --input /data/events.ndjson --output /data/training-samples \
  --window-hours 168 --allowed-lateness-minutes 10
```

Kafka mode:

```bash
flink run -c io.openeventflow.recommendation.flink.RecommendationAttributionJob \
  target/flink-recommendation-0.1.0-SNAPSHOT.jar \
  --bootstrap-servers kafka.recsys-infra.svc.cluster.local:9092 \
  --input-topic recsys.behavior-events.v1 \
  --output-topic recsys.training-samples.v1 \
  --group-id openeventflow-recommendation-attribution \
  --source-idle-seconds 60
```

For bounded integration tests, `--window-seconds` and `--allowed-lateness-seconds` override the production-scale options. Idle Kafka partitions are excluded from the global watermark after `--source-idle-seconds`.

The shaded JAR includes Kafka and Jackson dependencies; Flink runtime dependencies remain provided by the cluster. Its default main class is `RecommendationAttributionJob`; use Flink's `-c` option to select `AttributionJob` or `RealtimeInterestJob`. All Kafka outputs use at-least-once delivery, so consumers must upsert or deduplicate by output key. Configure durable checkpoint storage and the RocksDB state backend in production.

# Flink recommendation jobs

Java 17 / Apache Flink 1.20 example jobs for recommendation attribution and decayed realtime interest profiles.

`AttributionJob` consumes JSON `Event` records from `snowplow_enriched_events`, writes JSON training samples to `openeventflow_training_samples`, and routes events behind the watermark to `openeventflow_late_events`. Configure brokers and topics with `KAFKA_BOOTSTRAP_SERVERS`, `INPUT_TOPIC`, `TRAINING_TOPIC`, and `LATE_TOPIC`. It uses bounded-out-of-orderness event-time watermarks, checkpointing, restart defaults, TTL-backed event-id deduplication, and impression expiry.

`RealtimeInterestJob` writes event-time-windowed profiles to `openeventflow_interest_profiles` by default. Configure `FEATURE_TOPIC` and `INTEREST_WINDOW_MS`, or call `RealtimeInterestJob.run(FeatureSink)` to inject an idempotent deployment-specific feature-store/gRPC adapter.

Malformed records are routed to `openeventflow_parse_errors` (`PARSE_ERROR_TOPIC`). Kafka sinks use at-least-once delivery, so output identifiers and injected feature writes must remain idempotent.

Build with `mvn -f streaming/flink-recommendation/pom.xml clean package`. The shaded job JAR includes Kafka/Jackson dependencies; Flink runtime dependencies remain provided by the cluster.

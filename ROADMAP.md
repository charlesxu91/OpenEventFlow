# Roadmap

OpenEventFlow is currently at the first open-source release stage. The core SDK, collector, warehouse, code generation, deployment templates, and e2e tests are implemented.

## Near Term

- Add persistent SDK queues:
  - IndexedDB for Web
  - Room or SQLite for Android
  - SQLite for iOS
  - sqflite or platform storage for Flutter
- Add retry, backoff, and dead-letter examples for stream consumers.
- Add a production Kafka producer adapter with idempotence, backpressure, and bounded retries.
- Add OpenTelemetry metrics for collector and warehouse loader services.
- Add more generated examples for ecommerce and short-video tracking plans.

## Medium Term

- Add framework-level auto-tracking adapters for common navigation and exposure patterns.
- Expand Flink attribution examples with deployment-specific feature-service gRPC clients and operational dashboards.
- Add Iceberg or lakehouse warehouse examples.
- Add data quality checks for freshness, volume, schema drift, and bad-event rate.
- Add privacy controls for regional routing, field redaction, and user deletion workflows.

## Long Term

- Add a lightweight governance UI for tracking-plan review.
- Add SDK conformance test suites that app teams can run in their own repos.
- Add migration guides from Firebase Analytics, Segment, Snowplow trackers, and custom in-house SDKs.
- Add examples for gaming, media, SaaS, and marketplace event models.

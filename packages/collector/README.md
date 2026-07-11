# @openeventflow/collector

The collector validates tracking-plan events and durably acknowledges accepted HTTP batches only after the configured broker has acknowledged the corresponding raw and enriched/bad Kafka records.

## Production Kafka runtime

Set the following environment variables when starting `src/server.js`:

```text
BROKER_TYPE=kafka
KAFKA_BROKERS=kafka-0.kafka:9092,kafka-1.kafka:9092
KAFKA_CLIENT_ID=openeventflow-collector
KAFKA_COMPRESSION=gzip
MAX_BODY_BYTES=1048576
MAX_BATCH_SIZE=500
```

The Kafka adapter enables idempotent production, disables implicit topic creation, uses `acks=-1`, groups each Collector request into a compressed multi-topic batch, and keys records by `event_id`. Topic creation and replication settings remain an infrastructure responsibility.

Operational endpoints:

- `GET /health/live` reports process liveness.
- `GET /health/ready` verifies that the configured broker can connect.
- `POST /collect` only accepts `application/json` and returns `202` after broker acknowledgement. Broker failures return `503` so clients can retry with the same event IDs.

`createCollectorRuntime().close()` stops accepting connections, waits for active HTTP requests through Node's server close behavior, and disconnects the Kafka producer. The executable installs the same shutdown behavior for `SIGTERM` and `SIGINT`.

Small collector building blocks for local development, E2E tests, and self-hosted deployments.

It validates incoming normalized OpenEventFlow events against a tracking plan, writes all input to a raw topic, routes valid events to an enriched topic, and routes invalid events to a bad-events topic.

```js
const {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry
} = require("@openeventflow/collector");
```

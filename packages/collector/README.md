# @openeventflow/collector

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

The HTTP runtime supports `COLLECTOR_API_KEY`, `MAX_BODY_BYTES`, `GET /healthz`, and `GET /readyz`. Health endpoints remain unauthenticated for orchestrator probes. Broker adapters may return a promise from `publish`; the collector waits for it before acknowledging the HTTP batch and maps publish failures to `503 Service Unavailable`.

The bundled in-memory broker is intended only for tests and local development. Its acknowledgement is not a durable Kafka or Redpanda acknowledgement. Production deployments should inject a Kafka adapter configured for the required durability, retry, and idempotence guarantees.

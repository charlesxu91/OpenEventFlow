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

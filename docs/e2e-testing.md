# E2E Testing

The e2e suite validates complete ecommerce and short-video behavior paths:

```text
sample ecommerce app
sample short-video feed app
  -> OpenEventFlow SDK
  -> local collector
  -> raw topic
  -> schema validation
  -> enriched topic or bad-events topic
  -> warehouse fact tables
```

Covered behaviors:

- product exposure
- product click
- page stay
- add to cart
- video feed exposure
- video autoplay/play
- video watch duration and completion
- video like/share engagement
- invalid event routing to bad events
- browser UI triggered warehouse consistency

Run:

```bash
npm test
```

## Why The Test Uses In-Memory Middleware

The CI e2e test uses in-memory implementations of the collector, Kafka-like broker, and warehouse so it can run without Docker, Kubernetes, network access, or image pulls.

The behavior mirrors the production middleware boundaries:

| E2E Component | Production Equivalent |
| --- | --- |
| `LocalCollector` | Snowplow Collector / project collector |
| `InMemoryTopicBroker` | Kafka / Redpanda |
| `TrackingPlanRegistry` | Iglu / schema registry |
| `Warehouse` | ClickHouse / lakehouse / warehouse |
| ecommerce consumer | Flink / Spark / Kafka Streams / Snowbridge consumer |

## Real Middleware Templates

For local integration work with real middleware, use:

```bash
npm run smoke:docker
npm run smoke:docker:video
```

These start Redpanda and ClickHouse, produce ecommerce or short-video feed behavior events, consume them back from Redpanda, load them through the warehouse loader, and verify ClickHouse fact/ADS counts.

Kubernetes manifests are under `deploy/k8s`.

Those templates provide Redpanda and ClickHouse as middleware targets. The automated unit/e2e test remains in-memory by design.

## Browser UI Scenario

The browser UI scenario lives in `examples/ecommerce-ui`.

Open `examples/ecommerce-ui/index.html`, then:

1. Wait for the product card exposure to be recorded.
2. Click the product card.
3. Click `Add to cart`.
4. Inspect the warehouse snapshot.

Expected final fact counts:

```json
{
  "exposures": 1,
  "clicks": 1,
  "stays": 1,
  "carts": 1
}
```

## Short-Video Feed Scenario

The short-video feed scenario validates:

```json
{
  "exposures": 1,
  "plays": 1,
  "watches": 1,
  "engagements": 2
}
```

The Docker smoke also verifies ADS aggregates for watch time, completion, likes, and shares.

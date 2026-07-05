# Design Principles

OpenEventFlow is built for teams that treat behavior events as production data contracts.

## 1. Contract First

Events start in a tracking plan. The plan defines the event name, owner, description, schema version, required fields, and field types. The CLI then generates JSON Schema and typed event models for supported SDKs.

This keeps event changes reviewable before an app release and gives data teams a stable contract for downstream models.

## 2. SDKs Stay Lightweight

The SDK is responsible for collecting and normalizing events, adding identity and session context, honoring consent, queueing events, and flushing batches. It does not own reports, funnels, cohorts, or dashboards.

This keeps SDK adoption safe for large apps and avoids coupling app releases to analytics-product decisions.

## 3. Collector-Mediated Ingestion

Clients send events to a collector. They do not connect directly to Kafka, Redpanda, ClickHouse, or a warehouse.

The collector is the control point for schema validation, bad-event routing, authentication, throttling, topic routing, and protocol compatibility.

## 4. Lifecycle-Aware Duration

Stay and watch duration are not naive `end - start` timers. They must exclude background time, inactive time, and paused playback. SDK APIs expose begin, pause, resume, end, switch, cancel, and flush operations so apps can model real foreground engagement.

## 5. Warehouse-Friendly Semantics

OpenEventFlow maps event streams into explicit warehouse layers:

- ODS keeps raw normalized event records.
- DWD keeps cleaned behavior events.
- Fact tables model user actions such as exposures, clicks, stays, carts, plays, watches, and engagements.
- ADS tables aggregate consumer-ready metrics.

The goal is to make behavior data reliable for BI, recommendation systems, experimentation, ads, and other downstream consumers.

## 6. Snowplow-Native, Not Snowplow-Locked

OpenEventFlow uses Snowplow-style self-describing events and schema governance, but keeps interfaces modular. Teams can use the included collector and warehouse loader, or adapt the SDK and generated schemas to existing Snowplow, Kafka, Redpanda, ClickHouse, lakehouse, or CDP pipelines.

## 7. E2E Verification Over SDK Demos

An SDK example is not enough. OpenEventFlow includes e2e tests for ecommerce and short-video feed scenarios that verify the same user action from UI trigger through collector, stream, warehouse fact table, and ADS aggregate.

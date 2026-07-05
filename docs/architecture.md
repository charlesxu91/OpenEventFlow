# Architecture

OpenEventFlow is designed as a Snowplow-native mobile analytics engineering layer.

## Responsibilities

OpenEventFlow owns:

- Mobile-facing analytics APIs
- Tracking plan validation
- JSON Schema and mobile event type generation
- Autotracking conventions
- Identity, session, consent, and debug API shape
- Local pipeline examples and warehouse modeling guidance

Snowplow owns:

- Tracker protocol
- Collector ingestion
- Iglu schema validation
- Enrichment
- Good and bad event streams
- Kafka/Kinesis/PubSub/SQS sinks

## Data Flow

```text
Business code
  -> OpenEventFlow.track(ProductExposed)
  -> Snowplow self-describing event
  -> Snowplow Collector
  -> raw stream
  -> Snowplow Enrich + Iglu
  -> enriched stream or bad events
  -> Flink / Snowbridge / consumers
  -> ClickHouse / lakehouse / warehouse
```

## Design Rules

- Business apps do not call Snowplow classes directly.
- Tracking plans are reviewed before SDK code is generated.
- Events must have versioned schemas.
- Invalid events are routed to bad events instead of silently entering the main stream.
- Autotracking is modular and conservative by default.
- Exposure semantics are explicit and measurable.

## Event Contract

Each event has:

- `name`
- `version`
- `owner`
- `description`
- required fields
- typed properties
- generated JSON Schema
- generated Kotlin/Swift event type
- Snowplow Iglu URI

Example Iglu URI:

```text
iglu:io.openeventflow/product_exposed/jsonschema/1-0-0
```

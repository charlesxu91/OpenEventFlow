# Production Readiness and Recommendation Streaming Design

## Goal

Turn OpenEventFlow's current first-release event pipeline into a production-shaped foundation that can truthfully demonstrate collector hardening, recommendation attribution, Flink real-time processing, and tracking-plan compatibility governance.

The work must keep module boundaries explicit. OpenEventFlow owns event contracts, collection, stream processing examples, and training-sample outputs. It does not become an online recommendation API, model server, search engine, or feature database.

## Scope

### Collector hardening

The HTTP collector will add:

- API-key authentication with constant-time comparison when a key is configured
- unauthenticated liveness and readiness endpoints
- configurable request-body size limits
- awaited broker publishing so a successful HTTP response means every required publish has been acknowledged by the configured broker adapter
- explicit service-unavailable responses for broker failures
- Kubernetes Deployment, Service, HPA, and PDB examples

The in-memory broker remains a test and local-development adapter. Documentation must not describe it as durable Kafka acknowledgement.

### Recommendation event contract

Recommendation-related events share the following correlation dimensions:

- `request_id`
- `impression_id`
- `item_type`
- `item_id`
- `rank_position`
- `model_version`
- `strategy_id`
- `experiment_id`

The tracking plan will include recommendation delivery, impression, click, add-to-cart, payment, and refund events. Optional commerce identifiers such as order and payment IDs will support correction events without making item identity product-specific.

### Attribution semantics

The first runnable attribution implementation will:

- deduplicate by `event_id`
- correlate behavior to an impression using `impression_id` and item identity
- apply a configurable event-time attribution window
- emit positive click, cart, and payment labels
- emit a negative sample when an impression expires without a configured positive action
- emit refund correction samples that reference the original attributed conversion
- retain request, model, strategy, experiment, position, and event-time context in training samples

Late events beyond the configured allowance are routed to a late-event output rather than silently changing finalized samples.

### Flink jobs

A Maven-based `streaming/flink-recommendation` module will provide:

- `AttributionJob` for event-time parsing, watermarks, deduplication, keyed attribution state, timers, training-sample output, and late events
- `RealtimeInterestJob` for decayed user-interest signals by category, brand, price bucket, content type, and action
- Kafka/Redpanda source and sink configuration
- checkpoint, restart, and state-TTL defaults
- a feature-sink interface with a gRPC-shaped adapter boundary; deployment-specific generated protobuf clients are injected rather than coupled to this repository
- unit tests for pure attribution and interest rules plus Flink operator tests where the available dependencies support them

The implementation targets a documented Flink and Java version and must compile with Maven. It is an example production foundation, not a claim of operating a complete recommendation platform.

### Schema evolution governance

The tracking-plan CLI will gain a compatibility command that compares a candidate plan with a baseline plan. It will classify changes as:

- compatible: additive optional events or fields and documentation-only changes
- deprecated: events or fields retained in the contract but marked with deprecation metadata
- breaking: removing events, removing previously supported fields, adding required fields without a safe default, changing field types, or changing schema identity/version incompatibly

The command will return a non-zero exit status for breaking changes so CI can block them. Tracking-plan validation will recognize deprecation metadata, and documentation will define versioning, minimum deprecation periods, and consumer migration expectations.

## Data flow

```text
App SDK
  -> authenticated Collector
  -> Redpanda raw / valid / bad-event topics
     -> AttributionJob
        -> training-samples / late-events topics
     -> RealtimeInterestJob
        -> feature sink boundary
     -> warehouse loader
        -> ClickHouse / dbt
```

## Error handling

- Authentication failures return `401` without parsing or publishing the payload.
- Oversized requests return `413` and terminate body accumulation.
- Invalid JSON or invalid event batches return `400`.
- Broker publication failures return `503`; the collector does not report the batch as accepted.
- Schema-invalid events continue to be acknowledged as collected and are published to the bad-event topic.
- Flink parsing failures, late events, and attribution corrections use explicit side outputs or dedicated topics.
- Compatibility-check failures include machine-readable change codes and human-readable paths.

## Testing and verification

Work follows test-first development:

1. Add failing Node tests for collector authentication, health, body limits, awaited acknowledgement, and broker failures.
2. Add failing CLI tests for compatible, deprecated, and breaking tracking-plan changes.
3. Add failing attribution-domain tests before implementing correlation and correction rules.
4. Add failing Java tests before implementing Flink job logic.
5. Run targeted tests after each slice, then `npm run verify`, Maven tests/package, manifest validation, and documentation consistency searches.

## Delivery

All changes are delivered on `fix/production-readiness`. The branch will contain focused commits, be pushed to `origin`, and will not claim that persistent mobile queues, OpenTelemetry dashboards, complete privacy governance, or production infrastructure operations have been implemented unless they are separately verified.


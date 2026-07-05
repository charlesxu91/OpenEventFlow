# OpenEventFlow Android SDK

The Android SDK exposes a stable app-facing API and adapts events to Snowplow under the hood.

Planned modules:

- `openeventflow-core`
- `openeventflow-snowplow-adapter`
- `openeventflow-autotrack`
- `openeventflow-commerce`
- `openeventflow-debug`

Business code should call OpenEventFlow APIs instead of Snowplow tracker classes directly.

## Current Implementation

The package includes:

- `Analytics` public API
- `OpenEventFlowClient`
- `MemoryEventStore`
- injected `AnalyticsTransport`
- consent handling
- identity context
- session context
- PII key filtering
- stay-duration tracking with `beginStay`, `pauseStay`, `resumeStay`, `endStay`, `switchStay`, `cancelStay`, and `flushActiveStays`

## Stay Duration

```kotlin
client.beginStay("screen", mapOf("page" to "product_detail", "product_id" to "sku-1"))
client.pauseStay("screen")
client.resumeStay("screen")
client.endStay("screen", exitReason = "add_to_cart")
client.flush()
```

# OpenEventFlow iOS SDK

The iOS SDK exposes a stable app-facing API and adapts events to Snowplow under the hood.

Planned modules:

- `OpenEventFlowCore`
- `OpenEventFlowSnowplowAdapter`
- `OpenEventFlowAutotrack`
- `OpenEventFlowCommerce`
- `OpenEventFlowDebug`

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

```swift
client.beginStay(key: "screen", properties: ["page": "product_detail", "product_id": "sku-1"])
client.pauseStay(key: "screen")
client.resumeStay(key: "screen")
client.endStay(key: "screen", exitReason: "add_to_cart")
client.flush()
```

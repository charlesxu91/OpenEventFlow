# Recommendation data and attribution pipeline

OpenEventFlow is the client-event and attribution boundary for recommendation platforms. Recommendation serving remains a separate latency-sensitive system. The dependency-free `@openeventflow/recommendation` package turns normalized events into attribution samples and decayed interest signals; it does not provide online ranking, model serving, or feature storage.

```text
Web/mobile SDK or authoritative backend producer
  -> OpenEventFlow Collector
  -> raw / validated / bad-event Kafka topics
  -> Flink recommendation attribution and realtime interest
  -> training samples / feature sink
  -> warehouse or object-storage training datasets
```

## Attribution input

Create an engine with explicit timing and label policy:

```js
const engine = createAttributionEngine({
  attributionWindowMs: 86_400_000,
  allowedLatenessMs: 300_000,
  positiveActions: [
    "recommendation_click",
    "recommendation_add_to_cart",
    "recommendation_payment"
  ]
});
```

`process(event)` consumes one normalized object. Every event requires non-empty string values for `event_id`, `event_name`, `request_id`, `impression_id`, `item_type`, and `item_id`, plus a finite numeric event-time `timestamp`. Attribution identity is the full tuple `request_id`, `impression_id`, `item_type`, and `item_id`.

An impression uses `event_name: "recommendation_impression"` and may carry `user_id`, `rank_position`, `model_version`, `strategy_id`, and `experiment_id`. Positive events use a name listed in `positiveActions` and the same correlation tuple. A `recommendation_payment` may include `order_id` and `payment_id`. A `recommendation_refund` uses the original correlation tuple and should include `payment_id` or `order_id` so it can resolve the original payment deterministically.

`process(event)` and `advanceWatermark(timestamp)` always return this shape:

```js
{
  samples: [],
  corrections: [],
  lateEvents: [],
  duplicates: []
}
```

- `samples` contains positive samples immediately and negative samples after watermark expiry. A sample preserves request, impression, item, ranking, model, strategy, experiment, user, and event-time context.
- `corrections` contains refund records whose `original_sample_id` references the attributed payment sample.
- `lateEvents` contains events with `timestamp < watermark - allowedLatenessMs`; these events do not mutate attribution or deduplication state, so a repeated late delivery remains a late event.
- `duplicates` contains repeated `event_id` inputs; duplicates do not mutate attribution state.

`advanceWatermark(timestamp)` must be called with non-decreasing event-time progress. An unconverted impression is finalized as a negative sample only after the watermark passes `impression timestamp + attributionWindowMs + allowedLatenessMs`. Sample IDs are deterministic functions of correlation identity, action, and source event or expiry time.

State is in memory. Callers that require recovery or distributed processing must provide that operational state boundary, such as the repository's Flink example.

## Interest input and output

Create a profile with an explicit decay policy:

```js
const profile = createInterestProfile({ halfLifeMs: 3_600_000 });
profile.apply({
  timestamp: 1_700_000_000_000,
  action: "recommendation_click",
  category: "sports",
  brand: "example",
  price_bucket: "20-50",
  content_type: "video",
  weight: 1
});
```

`timestamp` and `action` are required. `category`, `brand`, `price_bucket`, and `content_type` are optional strings. `weight` is an optional finite number and defaults to `1`.

`snapshot(at)` requires an explicit timestamp at or after every applied event and returns:

```js
{
  at,
  categories: { sports: 0.5 },
  brands: { example: 0.5 },
  priceBuckets: { "20-50": 0.5 },
  contentTypes: { video: 0.5 },
  actions: { recommendation_click: 0.5 }
}
```

Each contribution is `weight * 2 ** (-(at - timestamp) / halfLifeMs)`. The implementation never reads wall-clock time, so replaying the same events and snapshot timestamp produces the same values.

## Event ownership and production topics

Server-side delivery events and authoritative commerce events use the same canonical envelope and enter the validated behavior topic through an authenticated Collector or compatible backend producer. A browser or mobile client must never be treated as the source of truth for payment, cancellation, refund, price, or net GMV.

## Attribution identity

Recommendation events carry the generic correlation tuple `request_id + impression_id + item_type + item_id`. Product-specific pipelines may map `item_id` to `product_id` at their boundary.

`delivery_id`, surface, position, candidate source, model version, feature-set
version, experiment assignment, and recommendation generation are retained as
provenance. `event_id` is the transport deduplication key.

## Event ownership

| Event | Producer | Meaning |
| --- | --- | --- |
| `recommendation_delivered` | recommendation server | candidate returned to the client |
| `product_impressed` | client SDK | item met the visible-area and duration policy |
| click/favorite/cart | client SDK plus backend reconciliation | user action |
| order/payment/cancel/refund | commerce backend or CDC | authoritative transaction label |

## Delivery guarantees

- The Collector returns HTTP 202 only after the Kafka producer receives all
  acknowledgements.
- Clients retain `event_id` across retries. Kafka and Flink may deliver/process
  at least once, so downstream deduplication remains mandatory.
- Invalid contracts are routed to `recsys.client-bad.v1`; they are not silently
  coerced into the valid stream.
- Flink uses event time, bounded out-of-orderness, managed state, checkpointing,
  timers, and state cleanup before it is horizontally scaled.
- Negative labels are emitted only when the attribution window closes. Payment
  and refund updates may revise earlier labels through versioned/upsert output.

## Local k3s profile

The Collector deployment in `deploy/k3s` connects to
`kafka.recsys-infra.svc.cluster.local:9092`. The local profile uses small data
and replica counts to validate behavior. Production sizing, authentication,
TLS, registry integration, rack placement, and retention are supplied by the
production overlay.

```bash
./scripts/k3s-collector-up.sh
./scripts/k3s-flink-recommendation-up.sh
```

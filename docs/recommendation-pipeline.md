# Recommendation attribution core

`@openeventflow/recommendation` is a dependency-free event-time domain package. It turns normalized recommendation events into attribution samples and maintains decayed interest signals. It does not provide online ranking, model serving, or feature storage.

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

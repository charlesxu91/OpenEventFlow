# Mobile Engineering

Large apps need a stable analytics layer that survives screen rewrites, feature teams, app version drift, and offline usage.

## SDK Layers

```text
analytics-core
  Event model, queue, retry, flush, sampling, identity, session, consent.

analytics-snowplow-adapter
  Converts OpenEventFlow events to Snowplow self-describing events and contexts.

analytics-autotrack
  Screen, click, stay, and exposure helpers.

analytics-commerce
  Product exposure, product click, add to cart, order, payment.

analytics-debug
  Local event inspection, schema status, queue status, identity/session display.
```

## Autotracking Defaults

| Capability | Default | Reason |
| --- | --- | --- |
| Screen lifecycle | On | Low risk and useful for sessionization |
| Click autotracking | Off | High noise and privacy risk |
| Exposure autotracking | Explicit opt-in | Large apps need stable container/item semantics |
| Method swizzling | Off | Powerful but surprising |
| Bytecode instrumentation | Off | Requires explicit build-time ownership |

## Identity Rules

- Generate `anonymous_id` on first install.
- Attach `user_id` after login.
- Keep `anonymous_id` after logout.
- Start a new `session_id` after 30 minutes of inactivity.
- Attach app, device, user, session, and privacy contexts to each event.

## Exposure Rules

An exposure is counted only when:

- visible ratio is at least 50 percent
- visible duration reaches the configured threshold
- item id, container id, page, and exposure id are known

Recommended defaults:

```text
visible_ratio >= 0.5
duration_ms >= 1000
dedupe scope = session_id + exposure_id
```

## Queue Rules

- Write events to a local queue before upload.
- Upload in batches.
- Retry with exponential backoff.
- Flush on foreground/background transitions when the OS allows it.
- Drop low-priority events first when storage limits are reached.

Recommended priorities:

```text
P0: order, payment, add_to_cart
P1: click, search, login
P2: exposure, stay
P3: diagnostics
```

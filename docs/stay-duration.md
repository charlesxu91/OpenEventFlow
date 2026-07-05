# Stay Duration Tracking

Stay duration is measured in the shared core SDK so every framework can use the same semantics.

## Event Contract

The SDK emits `page_stay`:

```json
{
  "page": "product_detail",
  "stay_id": "stay-123",
  "duration_ms": 7300,
  "exit_reason": "add_to_cart"
}
```

Additional properties, such as `product_id`, `content_id`, `video_id`, or `recommend_trace_id`, are preserved.

## Timing Semantics

| Situation | SDK behavior |
| --- | --- |
| Page enters foreground | `beginStay(key, properties)` starts active timing. |
| App/browser goes background | `pauseStay(key)` freezes active timing. Background time is excluded. |
| App/browser returns foreground | `resumeStay(key)` resumes active timing. |
| Route or screen changes | `switchStay(key, nextProperties)` ends the previous stay with `route_change` and starts the next. |
| Component/page unmounts | `endStay(key, { exitReason })` emits one `page_stay` event. |
| App shutdown/pagehide | `flushActiveStays({ exitReason: "pagehide" })` ends all active stays, then `flush()` uploads queued events. |
| Very short view | Durations below `stay.minDurationMs` are dropped. Default: `1000ms`. |
| Abnormally long view | Duration is capped by `stay.maxDurationMs`. Default: `24h`. |
| Consent disabled | `page_stay` follows normal analytics consent and is not queued. |

## Core API

```js
const analytics = createAnalytics({
  app,
  transport,
  stay: {
    minDurationMs: 1000,
    maxDurationMs: 24 * 60 * 60 * 1000
  }
});

analytics.beginStay("screen", {
  page: "product_detail",
  product_id: "sku-100"
});

analytics.pauseStay("screen");
analytics.resumeStay("screen");

await analytics.endStay("screen", {
  exitReason: "add_to_cart"
});
```

## Web

`bindWebLifecycleStays` connects browser lifecycle events:

```js
bindWebLifecycleStays({
  analytics,
  document,
  window,
  stayKeys: ["screen"]
});
```

It pauses stays on `visibilitychange: hidden`, resumes on visible, and flushes active stays on `pagehide`.

## React

`useStay` starts on mount and ends on cleanup:

```js
useStay("screen", {
  page: "product_detail",
  product_id: product.id
}, {
  exitReason: "component_unmount"
});
```

## Native and Flutter Mapping

Mobile wrappers should map lifecycle callbacks to the same core model:

| Platform event | OpenEventFlow call |
| --- | --- |
| iOS `viewDidAppear` / Android `onResume` / Flutter route visible | `beginStay` or `resumeStay` |
| iOS `viewWillDisappear` / Android `onPause` / Flutter route hidden | `pauseStay` or `endStay` depending on route transition |
| App background | `pauseStay` |
| App foreground | `resumeStay` |
| App terminated or low-memory shutdown | `flushActiveStays` then `flush` |

Use stable keys per screen or content container. For example: `screen`, `video:<id>`, `product:<id>`, or `modal:<id>`.

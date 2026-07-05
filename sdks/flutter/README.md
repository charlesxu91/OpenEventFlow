# OpenEventFlow Flutter SDK

The Flutter SDK provides Dart primitives for tracking behavior events through the same OpenEventFlow contract used by native and JavaScript SDKs.

```dart
final client = OpenEventFlowClient(
  app: const OpenEventFlowAppContext(
    appId: 'shop-flutter',
    platform: 'flutter',
    appVersion: '1.0.0',
    sdkVersion: '0.1.0',
  ),
  transport: CallbackTransport((events) async {
    // Send to your collector or Snowplow adapter boundary.
  }),
);

await client.track(
  const SimpleAnalyticsEvent(
    name: 'add_to_cart',
    schema: 'iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0',
    properties: {'product_id': 'sku-1'},
  ),
);
await client.flush();
```

## Stay Duration

```dart
client.beginStay(
  'screen',
  properties: {'page': 'product_detail', 'product_id': 'sku-1'},
);

client.pauseStay('screen');
client.resumeStay('screen');

await client.endStay('screen', exitReason: 'add_to_cart');
await client.flush();
```

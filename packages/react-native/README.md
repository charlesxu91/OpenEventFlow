# @openeventflow/react-native

React Native SDK wrapper for OpenEventFlow.

```js
const { createReactNativeAnalytics } = require("@openeventflow/react-native");

const analytics = createReactNativeAnalytics({
  appId: "shop-app",
  appVersion: "1.0.0",
  endpoint: "https://collector.example.com/events"
});
```

## Stay Duration

Bind React Native `AppState` to foreground-only stay duration:

```js
const { AppState } = require("react-native");
const {
  bindReactNativeAppStateStays,
  createReactNativeAnalytics
} = require("@openeventflow/react-native");

const analytics = createReactNativeAnalytics({
  appId: "shop-app",
  appVersion: "1.0.0",
  endpoint: "https://collector.example.com/events"
});

const stay = bindReactNativeAppStateStays({
  analytics,
  appState: AppState,
  key: "screen",
  properties: { page: "product_detail" }
});

await stay.dispose({ exitReason: "screen_unmount" });
await analytics.flush();
```

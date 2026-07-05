const { createReactNativeAnalytics } = require("../packages/react-native/src/index");

const analytics = createReactNativeAnalytics({
  appId: "shop-app",
  appVersion: "1.0.0",
  endpoint: "https://collector.example.com/events"
});

analytics.screen("Home");

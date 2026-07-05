const { createWebAnalytics } = require("../packages/web/src/index");

const analytics = createWebAnalytics({
  appId: "shop-web",
  appVersion: "1.0.0",
  endpoint: "https://collector.example.com/events"
});

analytics.track({
  name: "product_exposed",
  schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
  properties: {
    product_id: "sku-123",
    page: "home",
    position: 1,
    exposure_id: "exp-123"
  }
});

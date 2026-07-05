# @openeventflow/web

Browser SDK for OpenEventFlow.

```js
const { createWebAnalytics } = require("@openeventflow/web");

const analytics = createWebAnalytics({
  appId: "shop-web",
  endpoint: "https://collector.example.com/events"
});

analytics.track({
  name: "product_exposed",
  schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
  properties: { product_id: "sku-1", position: 1 }
});
```

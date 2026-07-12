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

For production browsers, configure the persistent IndexedDB queue explicitly. Events remain queued across page reloads and are removed only after `transport.send` succeeds:

```js
const { createWebAnalytics, IndexedDBEventStore } = require("@openeventflow/web");

const analytics = createWebAnalytics({
  appId: "shop-web",
  endpoint: "https://collector.example.com/collect",
  store: new IndexedDBEventStore({
    databaseName: "shop-analytics",
    storeName: "pending-events"
  })
});
```

`push` and `remove` use IndexedDB `readwrite` transactions; `peek` and `size` use `readonly` transactions. FIFO order is provided by an auto-incremented sequence key. Applications should keep a single store instance and call `analytics.flush()` on their normal interval and lifecycle boundaries.

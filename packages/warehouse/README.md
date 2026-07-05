# @openeventflow/warehouse

Warehouse loading utilities for OpenEventFlow enriched events.

## Features

- Map enriched app behavior events into ODS, DWD, fact, and ADS rows.
- Insert rows through a generic adapter interface.
- Use `createClickHouseAdapter` with any ClickHouse client that exposes `insert()`.
- Use `createClickHouseHttpClient` for dependency-free ClickHouse HTTP inserts.
- Use `createKafkaWarehouseConsumer` with an injected Kafka client such as KafkaJS.
- Use `createInMemoryWarehouseAdapter` for deterministic tests.

## Tables

- `ods_snowplow_enriched_events`
- `dwd_app_behavior_events`
- `fact_product_exposures`
- `fact_product_clicks`
- `fact_page_stays`
- `fact_cart_adds`
- `ads_product_behavior_daily`

## ClickHouse HTTP

```js
const {
  createClickHouseAdapter,
  createClickHouseHttpClient,
  createWarehouseLoader
} = require("@openeventflow/warehouse");

const loader = createWarehouseLoader({
  adapter: createClickHouseAdapter({
    database: "openeventflow",
    client: createClickHouseHttpClient({
      endpoint: "http://127.0.0.1:8123",
      username: "openeventflow",
      password: "openeventflow"
    })
  })
});
```

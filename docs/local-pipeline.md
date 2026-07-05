# Local Pipeline

The first local pipeline target is:

```text
OpenEventFlow example app
  -> Snowplow Collector
  -> Kafka or Redpanda
  -> Snowplow Enrich
  -> enriched events / bad events
  -> OpenEventFlow warehouse loader
  -> ClickHouse ODS / DWD / fact / ADS tables
```

## Topics

Recommended topics:

```text
snowplow_raw_events
snowplow_enriched_events
snowplow_bad_events
app_behavior_valid
app_exposure_events
app_click_events
app_cart_events
```

## Warehouse Tables

Implemented local ClickHouse tables:

```text
ods_snowplow_bad_events
ods_snowplow_enriched_events
dwd_app_behavior_events
fact_product_exposures
fact_product_clicks
fact_page_stays
fact_cart_adds
ads_product_behavior_daily
```

`packages/warehouse` contains the adapter-driven loader used by tests and production wiring. It can load enriched events directly into ClickHouse, while `warehouse/dbt` provides SQL-managed models for teams that prefer dbt transformations from ODS to DWD, fact, and ADS layers.

## Local Modeling Options

Use the JavaScript loader when you want a streaming consumer:

```text
Redpanda topic -> createKafkaWarehouseConsumer -> createWarehouseLoader -> ClickHouse
```

Use dbt when you want SQL-managed batch modeling:

```text
ClickHouse ODS -> dbt run -> DWD / fact / ADS models
```

## Docker Smoke Test

Run a local Redpanda plus ClickHouse end-to-end consistency check:

```bash
npm run smoke:docker
npm run smoke:docker:video
```

The scripts start `deploy/docker/docker-compose.yml`, create a unique Redpanda topic, produce ecommerce or short-video feed events, consume them back, load them through `@openeventflow/warehouse`, write ClickHouse, and verify the fact and ADS rows match the source events.

## Bad Event Handling

Bad events should be monitored as product quality signals:

- schema not found
- schema validation failed
- malformed event payload
- invalid app key or collector path
- blocked by privacy rules

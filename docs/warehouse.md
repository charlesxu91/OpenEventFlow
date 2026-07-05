# Warehouse Layer

OpenEventFlow includes a production-shaped warehouse layer for turning enriched behavior events into queryable ClickHouse tables.

## Data Flow

```text
App SDK
  -> Collector
  -> Redpanda or Kafka topic: snowplow_enriched_events
  -> @openeventflow/warehouse Kafka consumer
  -> ClickHouse ODS tables
  -> DWD behavior detail
  -> fact event tables
  -> ADS product behavior aggregates
```

## Layers

| Layer | Tables | Purpose |
| --- | --- | --- |
| ODS | `ods_snowplow_enriched_events`, `ods_snowplow_bad_events` | Store raw enriched payloads and rejected events for replay and audit. |
| DWD | `dwd_app_behavior_events` | Normalize identity, app, event time, event name, schema, and event properties. |
| FCT | `fact_product_exposures`, `fact_product_clicks`, `fact_page_stays`, `fact_cart_adds` | One table per core business behavior for downstream consumers. |
| ADS | `ads_product_behavior_daily` | Product-level daily exposure, click, cart, quantity, and GMV aggregates. |

Short-video feed behavior adds:

| Layer | Tables | Purpose |
| --- | --- | --- |
| FCT | `fact_video_exposures`, `fact_video_plays`, `fact_video_watches`, `fact_video_engagements` | Feed exposure, playback, watch duration/completion, and engagement actions. |
| ADS | `ads_video_behavior_daily` | Video-level daily exposure, play, watch time, completion, like, comment, share, and follow aggregates. |

## Runtime Module

The warehouse module lives in `packages/warehouse`.

```js
const {
  createClickHouseAdapter,
  createKafkaWarehouseConsumer,
  createWarehouseLoader
} = require("@openeventflow/warehouse");

const adapter = createClickHouseAdapter({
  database: "openeventflow",
  client: clickhouseClient
});

const loader = createWarehouseLoader({ adapter });

const consumer = createKafkaWarehouseConsumer({
  kafka,
  topic: "snowplow_enriched_events",
  groupId: "openeventflow-warehouse",
  loader
});

await consumer.start();
```

The module does not force a Kafka or ClickHouse client dependency. Production apps can inject KafkaJS and `@clickhouse/client`, while tests use in-memory adapters.

## dbt Project

The dbt project under `warehouse/dbt` provides a second modeling path for teams that prefer SQL-managed transformations:

```bash
cd warehouse/dbt
dbt deps
dbt run
dbt test
```

Use `warehouse/dbt/profiles.example.yml` as the starting profile for local ClickHouse.

## Local ClickHouse

`deploy/docker/init-clickhouse.sql` creates the ODS, DWD, fact, and ADS tables used by the local pipeline. The schema is intentionally event-contract aligned, so the same fields generated from `examples/tracking-plan.json` are visible in the fact tables.

## What This Is Not Yet

This is not a full DataWorks replacement. It does not include visual DAG development, release approvals, notebook execution, data catalog UI, or data permission workflows. Those should be composed later with DolphinScheduler or Airflow, dbt, OpenMetadata or DataHub, and Superset or Metabase.

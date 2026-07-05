const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const { createAnalytics, MemoryEventStore } = require("../../packages/core/src/index");
const {
  InMemoryTopicBroker,
  LocalCollector,
  TrackingPlanRegistry,
  Warehouse,
  createEcommerceWarehouseConsumer
} = require("../src/pipeline");
const { runEcommerceJourney } = require("../src/sample-ecommerce-app");

test("ecommerce SDK events flow through collector, broker, and warehouse facts", async () => {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.join(__dirname, "../../examples/tracking-plan.json"), "utf8")
  );
  const broker = new InMemoryTopicBroker();
  const registry = new TrackingPlanRegistry(trackingPlan);
  const collector = new LocalCollector({
    broker,
    registry,
    rawTopic: "snowplow_raw_events",
    validTopic: "snowplow_enriched_events",
    badTopic: "snowplow_bad_events"
  });
  const warehouse = new Warehouse();
  const consumer = createEcommerceWarehouseConsumer({ broker, warehouse, topic: "snowplow_enriched_events" });

  const analytics = createAnalytics({
    app: { appId: "shop-demo", platform: "react", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async (batch) => collector.collect(batch)
    },
    anonymousId: "anon-1",
    idGenerator: deterministicIds(["evt-exp", "evt-click", "evt-stay", "evt-cart", "session-2"]),
    clock: deterministicClock([900, 1000, 1100, 1200, 1300])
  });

  await runEcommerceJourney(analytics);
  const flushResult = await analytics.flush();
  await consumer.drain();

  assert.deepEqual(flushResult, { sent: 4, remaining: 0 });
  assert.equal(broker.topic("snowplow_raw_events").length, 4);
  assert.equal(broker.topic("snowplow_enriched_events").length, 4);
  assert.equal(broker.topic("snowplow_bad_events").length, 0);

  assert.equal(warehouse.table("ods_snowplow_enriched_events").length, 4);
  assert.equal(warehouse.table("dwd_app_behavior_events").length, 4);
  assert.equal(warehouse.table("fact_product_exposures").length, 1);
  assert.equal(warehouse.table("fact_product_clicks").length, 1);
  assert.equal(warehouse.table("fact_page_stays").length, 1);
  assert.equal(warehouse.table("fact_cart_adds").length, 1);
  assert.equal(warehouse.table("ads_product_behavior_daily").length, 1);

  assert.deepEqual(warehouse.table("fact_product_exposures")[0], {
    event_id: "evt-exp",
    event_date: "1970-01-01",
    user_id: "user-42",
    anonymous_id: "anon-1",
    product_id: "sku-100",
    page: "home_feed",
    position: 2,
    exposure_id: "exp-100",
    visible_ratio: 0.83,
    duration_ms: 1200,
    recommend_trace_id: "rec-abc",
    event_time: 1000
  });
  assert.equal(warehouse.table("fact_product_clicks")[0].click_id, "clk-100");
  assert.equal(warehouse.table("fact_page_stays")[0].duration_ms, 7300);
  assert.equal(warehouse.table("fact_cart_adds")[0].quantity, 1);
  assert.deepEqual(warehouse.table("ads_product_behavior_daily")[0], {
    event_date: "1970-01-01",
    product_id: "sku-100",
    exposures: 1,
    clicks: 1,
    cart_adds: 1,
    cart_quantity: 1,
    cart_gmv: 199
  });
});

test("collector routes schema-invalid ecommerce events to bad events", async () => {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.join(__dirname, "../../examples/tracking-plan.json"), "utf8")
  );
  const broker = new InMemoryTopicBroker();
  const collector = new LocalCollector({
    broker,
    registry: new TrackingPlanRegistry(trackingPlan)
  });

  await collector.collect([
    {
      event_id: "bad-1",
      event_name: "add_to_cart",
      schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
      client_time: 2000,
      properties: {
        product_id: "sku-100",
        quantity: "one"
      },
      context: { user: { anonymous_id: "anon-1" }, app: { app_id: "shop-demo" } }
    }
  ]);

  assert.equal(broker.topic("snowplow_enriched_events").length, 0);
  assert.equal(broker.topic("snowplow_bad_events").length, 1);
  assert.equal(broker.topic("snowplow_bad_events")[0].reason, "missing_required_property");
  assert.equal(broker.topic("snowplow_bad_events")[0].property, "sku_id");
});

function deterministicIds(values) {
  let index = 0;
  return () => values[index++] || `evt-${index}`;
}

function deterministicClock(values) {
  let index = 0;
  return () => values[index++] || values[values.length - 1];
}

const assert = require("node:assert/strict");
const test = require("node:test");
const {
  createClickHouseAdapter,
  createClickHouseHttpClient,
  createInMemoryWarehouseAdapter,
  createKafkaWarehouseConsumer,
  createWarehouseLoader
} = require("../src/index");

test("warehouse loader writes ODS, DWD, fact, and ADS rows for ecommerce events", async () => {
  const adapter = createInMemoryWarehouseAdapter();
  const loader = createWarehouseLoader({ adapter });

  const result = await loader.load([
    enrichedEvent("evt-exp", "product_exposed", {
      product_id: "sku-100",
      page: "home_feed",
      position: 2,
      exposure_id: "exp-100",
      visible_ratio: 0.83,
      duration_ms: 1200,
      recommend_trace_id: "rec-abc"
    }),
    enrichedEvent("evt-click", "product_clicked", {
      product_id: "sku-100",
      page: "home_feed",
      position: 2,
      click_id: "clk-100",
      recommend_trace_id: "rec-abc"
    }),
    enrichedEvent("evt-stay", "page_stay", {
      page: "product_detail",
      duration_ms: 7300,
      stay_id: "stay-100",
      exit_reason: "add_to_cart"
    }),
    enrichedEvent("evt-cart", "add_to_cart", {
      product_id: "sku-100",
      sku_id: "sku-100-red",
      quantity: 1,
      price: 199,
      currency: "CNY"
    })
  ]);

  assert.deepEqual(result, {
    loaded: 4,
    tables: {
      ods_snowplow_enriched_events: 4,
      dwd_app_behavior_events: 4,
      fact_product_exposures: 1,
      fact_product_clicks: 1,
      fact_page_stays: 1,
      fact_cart_adds: 1,
      ads_product_behavior_daily: 1
    }
  });
  assert.equal(adapter.table("ods_snowplow_enriched_events")[0].event_json.includes("\"event_id\":\"evt-exp\""), true);
  assert.deepEqual(adapter.table("dwd_app_behavior_events")[0], {
    event_id: "evt-exp",
    event_name: "product_exposed",
    schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
    user_id: "user-42",
    anonymous_id: "anon-1",
    app_id: "shop-demo",
    platform: "react",
    event_date: "1970-01-01",
    event_time: 1000,
    collector_time: 1010,
    enriched_at: 1020,
    properties: "{\"product_id\":\"sku-100\",\"page\":\"home_feed\",\"position\":2,\"exposure_id\":\"exp-100\",\"visible_ratio\":0.83,\"duration_ms\":1200,\"recommend_trace_id\":\"rec-abc\"}"
  });
  assert.equal(adapter.table("fact_product_exposures")[0].exposure_id, "exp-100");
  assert.equal(adapter.table("fact_product_clicks")[0].click_id, "clk-100");
  assert.equal(adapter.table("fact_page_stays")[0].stay_id, "stay-100");
  assert.equal(adapter.table("fact_cart_adds")[0].gmv, 199);
  assert.deepEqual(adapter.table("ads_product_behavior_daily")[0], {
    event_date: "1970-01-01",
    product_id: "sku-100",
    exposures: 1,
    clicks: 1,
    cart_adds: 1,
    cart_quantity: 1,
    cart_gmv: 199
  });
});

test("clickhouse adapter inserts JSONEachRow batches into configured database", async () => {
  const calls = [];
  const adapter = createClickHouseAdapter({
    database: "openeventflow",
    client: {
      async insert(call) {
        calls.push(call);
      }
    }
  });

  await adapter.insert("dwd_app_behavior_events", [
    {
      event_id: "evt-1",
      event_name: "add_to_cart",
      properties: "{\"quantity\":1}"
    }
  ]);

  assert.deepEqual(calls, [
    {
      table: "openeventflow.dwd_app_behavior_events",
      format: "JSONEachRow",
      values: [
        {
          event_id: "evt-1",
          event_name: "add_to_cart",
          properties: "{\"quantity\":1}"
        }
      ]
    }
  ]);
});

test("clickhouse http client posts JSONEachRow inserts with clickhouse credentials", async () => {
  const calls = [];
  const client = createClickHouseHttpClient({
    endpoint: "http://127.0.0.1:8123",
    username: "openeventflow",
    password: "openeventflow",
    fetch: async (url, request) => {
      calls.push({ url, request });
      return {
        ok: true,
        status: 200,
        text: async () => "Ok."
      };
    }
  });

  await client.insert({
    table: "openeventflow.fact_cart_adds",
    format: "JSONEachRow",
    values: [
      { event_id: "evt-1", product_id: "sku-1", quantity: 2 },
      { event_id: "evt-2", product_id: "sku-2", quantity: 1 }
    ]
  });

  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /query=INSERT%20INTO%20openeventflow\.fact_cart_adds%20FORMAT%20JSONEachRow/);
  assert.equal(calls[0].request.method, "POST");
  assert.equal(calls[0].request.headers["x-clickhouse-user"], "openeventflow");
  assert.equal(calls[0].request.headers["x-clickhouse-key"], "openeventflow");
  assert.equal(
    calls[0].request.body,
    "{\"event_id\":\"evt-1\",\"product_id\":\"sku-1\",\"quantity\":2}\n{\"event_id\":\"evt-2\",\"product_id\":\"sku-2\",\"quantity\":1}\n"
  );
});

test("kafka warehouse consumer loads parsed messages into the warehouse loader", async () => {
  const batches = [];
  const kafka = {
    consumer(config) {
      assert.deepEqual(config, { groupId: "openeventflow-warehouse" });
      return {
        async connect() {
          batches.push({ type: "connect" });
        },
        async subscribe(subscription) {
          batches.push({ type: "subscribe", subscription });
        },
        async run({ eachBatch }) {
          await eachBatch({
            batch: {
              messages: [
                {
                  value: Buffer.from(JSON.stringify(enrichedEvent("evt-cart", "add_to_cart", {
                    product_id: "sku-100",
                    sku_id: "sku-100-red",
                    quantity: 1,
                    price: 199,
                    currency: "CNY"
                  })))
                }
              ]
            }
          });
        }
      };
    }
  };
  const adapter = createInMemoryWarehouseAdapter();
  const loader = createWarehouseLoader({ adapter });
  const consumer = createKafkaWarehouseConsumer({
    kafka,
    topic: "snowplow_enriched_events",
    groupId: "openeventflow-warehouse",
    loader
  });

  await consumer.start();

  assert.deepEqual(batches, [
    { type: "connect" },
    {
      type: "subscribe",
      subscription: {
        topic: "snowplow_enriched_events",
        fromBeginning: false
      }
    }
  ]);
  assert.equal(adapter.table("fact_cart_adds").length, 1);
  assert.equal(adapter.table("ads_product_behavior_daily")[0].cart_gmv, 199);
});

function enrichedEvent(eventId, eventName, properties) {
  const clientTimeByName = {
    product_exposed: 1000,
    product_clicked: 1100,
    page_stay: 1200,
    add_to_cart: 1300
  };
  const client_time = clientTimeByName[eventName];
  return {
    event_id: eventId,
    event_name: eventName,
    schema: `iglu:io.openeventflow/${eventName}/jsonschema/1-0-0`,
    client_time,
    collector_time: client_time + 10,
    enriched_at: client_time + 20,
    properties,
    context: {
      user: {
        user_id: "user-42",
        anonymous_id: "anon-1"
      },
      app: {
        app_id: "shop-demo",
        platform: "react"
      }
    }
  };
}

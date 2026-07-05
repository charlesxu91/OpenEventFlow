const assert = require("node:assert/strict");
const { PassThrough } = require("node:stream");
const test = require("node:test");

const {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry
} = require("../src/index");

const trackingPlan = {
  schemaVendor: "io.openeventflow",
  events: [
    {
      name: "add_to_cart",
      version: "1-0-0",
      required: ["product_id", "sku_id", "quantity", "price"],
      properties: {
        product_id: { type: "string" },
        sku_id: { type: "string" },
        quantity: { type: "integer" },
        price: { type: "number" }
      }
    }
  ]
};

function validEvent(overrides = {}) {
  return {
    event_id: "evt-1",
    event_name: "add_to_cart",
    schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
    client_time: 1783231000000,
    properties: {
      product_id: "p-1",
      sku_id: "sku-1",
      quantity: 2,
      price: 19.9
    },
    context: {
      app: { app_id: "shop", platform: "web" },
      user: { anonymous_id: "anon-1", user_id: null }
    },
    ...overrides
  };
}

test("collector publishes raw and enriched events while routing schema failures to bad events", async () => {
  const broker = createInMemoryTopicBroker();
  const collector = createCollector({
    broker,
    registry: createTrackingPlanRegistry(trackingPlan),
    clock: () => 1783231001234
  });

  const result = await collector.collect([
    validEvent(),
    validEvent({ event_id: "evt-bad", properties: { product_id: "p-1" } })
  ]);

  assert.deepEqual(result, { accepted: 2, enriched: 1, bad: 1 });
  assert.equal(broker.topic("snowplow_raw_events").length, 2);
  assert.equal(broker.topic("snowplow_enriched_events").length, 1);
  assert.equal(broker.topic("snowplow_bad_events").length, 1);
  assert.equal(broker.topic("snowplow_enriched_events")[0].collector_time, 1783231001234);
  assert.equal(broker.topic("snowplow_bad_events")[0].reason, "missing_required_property");
});

test("http collector accepts a JSON event batch and returns collector counts", async () => {
  const broker = createInMemoryTopicBroker();
  const server = createHttpCollectorServer({
    collector: createCollector({
      broker,
      registry: createTrackingPlanRegistry(trackingPlan),
      clock: () => 1783231001234
    })
  });

  const response = await postJson(server, "/collect", {
    events: [validEvent({ event_id: "evt-http" })]
  });

  assert.equal(response.statusCode, 202);
  assert.deepEqual(JSON.parse(response.body), { accepted: 1, enriched: 1, bad: 0 });
  assert.equal(broker.topic("snowplow_enriched_events")[0].event_id, "evt-http");
});

function postJson(server, path, payload) {
  return new Promise((resolve, reject) => {
    const request = new PassThrough();
    request.method = "POST";
    request.url = path;
    request.headers = { "content-type": "application/json" };
    const response = {
      statusCode: 200,
      body: "",
      writeHead(statusCode) {
        this.statusCode = statusCode;
      },
      end(body) {
        this.body = body;
        resolve({ statusCode: this.statusCode, body: this.body });
      }
    };
    server.emit("request", request, response);
    request.on("error", reject);
    request.end(JSON.stringify(payload));
  });
}

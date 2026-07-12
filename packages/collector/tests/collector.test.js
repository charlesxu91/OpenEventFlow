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

test("collector awaits broker acknowledgements before accepting a batch", async () => {
  const published = [];
  const broker = {
    async publish(topic, message) {
      await new Promise((resolve) => setImmediate(resolve));
      published.push({ topic, message });
    }
  };
  const collector = createCollector({
    broker,
    registry: createTrackingPlanRegistry(trackingPlan)
  });

  const result = await collector.collect([validEvent()]);

  assert.deepEqual(result, { accepted: 1, enriched: 1, bad: 0 });
  assert.deepEqual(published.map(({ topic }) => topic), [
    "snowplow_raw_events",
    "snowplow_enriched_events"
  ]);
});

test("http collector exposes health and readiness without authentication", async () => {
  const server = createHttpCollectorServer({
    collector: { collect: async () => ({ accepted: 0, enriched: 0, bad: 0 }) },
    apiKey: "secret",
    readiness: async () => true
  });

  const health = await request(server, { method: "GET", path: "/healthz" });
  const ready = await request(server, { method: "GET", path: "/readyz" });

  assert.equal(health.statusCode, 200);
  assert.deepEqual(JSON.parse(health.body), { status: "ok" });
  assert.equal(ready.statusCode, 200);
  assert.deepEqual(JSON.parse(ready.body), { status: "ready" });
});

test("http collector requires the configured API key", async () => {
  const server = createHttpCollectorServer({
    collector: { collect: async () => ({ accepted: 0, enriched: 0, bad: 0 }) },
    apiKey: "secret"
  });

  const missing = await postJson(server, "/collect", { events: [] });
  const invalid = await postJson(server, "/collect", { events: [] }, { "x-api-key": "wrong" });
  const valid = await postJson(server, "/collect", { events: [] }, { "x-api-key": "secret" });

  assert.equal(missing.statusCode, 401);
  assert.equal(invalid.statusCode, 401);
  assert.equal(valid.statusCode, 202);
});

test("http collector rejects request bodies over the configured limit", async () => {
  const server = createHttpCollectorServer({
    collector: { collect: async () => ({ accepted: 0, enriched: 0, bad: 0 }) },
    maxBodyBytes: 10
  });

  const response = await postJson(server, "/collect", { events: [validEvent()] });

  assert.equal(response.statusCode, 413);
  assert.equal(JSON.parse(response.body).error, "payload_too_large");
});

test("http collector reports broker failures as unavailable", async () => {
  const server = createHttpCollectorServer({
    collector: createCollector({
      broker: { publish: async () => { throw new Error("broker unavailable"); } },
      registry: createTrackingPlanRegistry(trackingPlan)
    })
  });

  const response = await postJson(server, "/collect", { events: [validEvent()] });

  assert.equal(response.statusCode, 503);
  assert.equal(JSON.parse(response.body).error, "service_unavailable");
});

function postJson(server, path, payload, headers = {}) {
  return request(server, {
    method: "POST",
    path,
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(payload)
  });
}

function request(server, { method, path, headers = {}, body = "" }) {
  return new Promise((resolve, reject) => {
    const incoming = new PassThrough();
    incoming.method = method;
    incoming.url = path;
    incoming.headers = headers;
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
    server.emit("request", incoming, response);
    incoming.on("error", reject);
    incoming.end(body);
  });
}

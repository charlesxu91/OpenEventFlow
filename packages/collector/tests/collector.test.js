const assert = require("node:assert/strict");
const { PassThrough } = require("node:stream");
const test = require("node:test");

const {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createKafkaTopicBroker,
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

test("Kafka broker connects lazily and publishes a compressed idempotent batch with all acknowledgements", async () => {
  const calls = [];
  const producer = {
    async connect() { calls.push(["connect"]); },
    async sendBatch(batch) { calls.push(["sendBatch", batch]); },
    async disconnect() { calls.push(["disconnect"]); }
  };
  const producerOptions = [];
  const kafkaModule = {
    CompressionTypes: { GZIP: 1, None: 0 },
    Kafka: class {
      producer(options) {
        producerOptions.push(options);
        return producer;
      }
    }
  };
  const broker = createKafkaTopicBroker({
    kafkaModule,
    brokers: ["kafka:9092"],
    clientId: "collector-test"
  });

  await broker.publishBatch([
    { topic: "raw", key: "evt-1", message: { event_id: "evt-1" } },
    { topic: "valid", key: "evt-1", message: { event_id: "evt-1", valid: true } }
  ]);
  await broker.close();

  assert.deepEqual(producerOptions[0], {
    idempotent: true,
    maxInFlightRequests: 5,
    allowAutoTopicCreation: false,
    transactionTimeout: 30000
  });
  assert.equal(calls[0][0], "connect");
  assert.equal(calls[1][1].acks, -1);
  assert.equal(calls[1][1].compression, 1);
  assert.deepEqual(calls[1][1].topicMessages.map((entry) => entry.topic), ["raw", "valid"]);
  assert.equal(calls[1][1].topicMessages[0].messages[0].key, "evt-1");
  assert.equal(calls[2][0], "disconnect");
});

test("http collector enforces media type, body and batch limits", async () => {
  const server = createHttpCollectorServer({
    collector: { async collect(events) { return { accepted: events.length }; } },
    maxBodyBytes: 64,
    maxBatchSize: 1
  });

  const media = await request(server, "POST", "/collect", "{}", { "content-type": "text/plain" });
  assert.equal(media.statusCode, 415);
  const batch = await request(server, "POST", "/collect", JSON.stringify([{}, {}]), { "content-type": "application/json" });
  assert.equal(batch.statusCode, 413);
  const body = await request(server, "POST", "/collect", JSON.stringify({ events: [{ value: "x".repeat(80) }] }), { "content-type": "application/json" });
  assert.equal(body.statusCode, 413);
});

test("http collector exposes liveness and broker-backed readiness", async () => {
  const server = createHttpCollectorServer({
    collector: {
      async collect() { return {}; },
      async health() { return { ready: false, reason: "kafka_unavailable" }; }
    }
  });
  assert.equal((await request(server, "GET", "/health/live", "")).statusCode, 200);
  const readiness = await request(server, "GET", "/health/ready", "");
  assert.equal(readiness.statusCode, 503);
  assert.equal(JSON.parse(readiness.body).reason, "kafka_unavailable");
});

test("http collector rejects missing API keys when authentication is configured", async () => {
  const server = createHttpCollectorServer({
    collector: { async collect(events) { return { accepted: events.length }; } },
    apiKeys: ["secret-key"]
  });
  const denied = await postJson(server, "/collect", [{}]);
  assert.equal(denied.statusCode, 401);
  const accepted = await request(server, "POST", "/collect", JSON.stringify([{}]), {
    "content-type": "application/json",
    "x-api-key": "secret-key"
  });
  assert.equal(accepted.statusCode, 202);
});

function postJson(server, path, payload) {
  return request(server, "POST", path, JSON.stringify(payload), { "content-type": "application/json" });
}

function request(server, method, path, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const request = new PassThrough();
    request.method = method;
    request.url = path;
    request.headers = headers;
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
    request.end(body);
  });
}

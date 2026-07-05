const assert = require("node:assert/strict");
const test = require("node:test");

test("createAnalytics queues events, adds identity context, and flushes batches", async () => {
  const { createAnalytics, MemoryEventStore } = require("../src/index");
  const sent = [];
  const analytics = createAnalytics({
    app: { appId: "shop", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async (batch) => {
        sent.push(batch);
        return { ok: true };
      }
    },
    idGenerator: () => "event-1",
    clock: () => 123456
  });

  analytics.identify("user-123", { tier: "gold" });
  await analytics.track({
    name: "product_exposed",
    schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
    properties: { product_id: "sku-1", position: 1 }
  });

  assert.equal(await analytics.queueSize(), 1);
  const result = await analytics.flush();

  assert.deepEqual(result, { sent: 1, remaining: 0 });
  assert.equal(sent.length, 1);
  assert.equal(sent[0][0].event_id, "event-1");
  assert.equal(sent[0][0].event_name, "product_exposed");
  assert.equal(sent[0][0].context.user.user_id, "user-123");
  assert.equal(sent[0][0].context.app.platform, "web");
});

test("createAnalytics drops events when analytics consent is disabled", async () => {
  const { createAnalytics, MemoryEventStore } = require("../src/index");
  const analytics = createAnalytics({
    app: { appId: "shop", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async () => {
        throw new Error("transport should not be called");
      }
    }
  });

  analytics.setConsent({ analyticsAllowed: false });
  const result = await analytics.track({
    name: "blocked",
    schema: "iglu:io.openeventflow/blocked/jsonschema/1-0-0",
    properties: {}
  });

  assert.deepEqual(result, { accepted: false, reason: "analytics_consent_disabled" });
  assert.equal(await analytics.queueSize(), 0);
});

test("stay tracker records active foreground duration and excludes background time", async () => {
  const { createAnalytics, MemoryEventStore } = require("../src/index");
  const sent = [];
  const clock = deterministicClock([0, 1000, 1800, 4800, 6500]);
  const analytics = createAnalytics({
    app: { appId: "shop", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async (batch) => {
        sent.push(...batch);
      }
    },
    idGenerator: deterministicIds(["stay-1", "event-1"]),
    clock
  });

  analytics.beginStay("product-detail", {
    page: "product_detail",
    product_id: "sku-100"
  });
  analytics.pauseStay("product-detail");
  analytics.resumeStay("product-detail");
  const result = await analytics.endStay("product-detail", { exitReason: "add_to_cart" });
  await analytics.flush();

  assert.deepEqual(result, { accepted: true, eventId: "event-1", durationMs: 2500 });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].event_name, "page_stay");
  assert.equal(sent[0].properties.page, "product_detail");
  assert.equal(sent[0].properties.product_id, "sku-100");
  assert.equal(sent[0].properties.stay_id, "stay-1");
  assert.equal(sent[0].properties.duration_ms, 2500);
  assert.equal(sent[0].properties.exit_reason, "add_to_cart");
});

test("stay tracker switches pages by ending the previous page before starting the next", async () => {
  const { createAnalytics, MemoryEventStore } = require("../src/index");
  const sent = [];
  const analytics = createAnalytics({
    app: { appId: "shop", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async (batch) => {
        sent.push(...batch);
      }
    },
    idGenerator: deterministicIds(["stay-home", "event-home", "stay-detail", "event-detail"]),
    clock: deterministicClock([0, 1000, 3600, 4000, 8200])
  });

  analytics.beginStay("screen", { page: "home_feed" });
  const switchResult = await analytics.switchStay("screen", { page: "product_detail" });
  const endResult = await analytics.endStay("screen", { exitReason: "back" });
  await analytics.flush();

  assert.deepEqual(switchResult, { accepted: true, eventId: "event-home", durationMs: 2600 });
  assert.deepEqual(endResult, { accepted: true, eventId: "event-detail", durationMs: 4200 });
  assert.deepEqual(sent.map((event) => event.properties.page), ["home_feed", "product_detail"]);
  assert.deepEqual(sent.map((event) => event.properties.exit_reason), ["route_change", "back"]);
});

test("stay tracker drops too-short stays and can flush active stays on shutdown", async () => {
  const { createAnalytics, MemoryEventStore } = require("../src/index");
  const sent = [];
  const analytics = createAnalytics({
    app: { appId: "shop", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: {
      send: async (batch) => {
        sent.push(...batch);
      }
    },
    idGenerator: deterministicIds(["stay-short", "stay-valid", "event-valid"]),
    clock: deterministicClock([0, 1000, 1300, 2000, 5600]),
    stay: { minDurationMs: 1000 }
  });

  analytics.beginStay("toast", { page: "toast" });
  const shortResult = await analytics.endStay("toast", { exitReason: "dismiss" });
  analytics.beginStay("screen", { page: "checkout" });
  const flushed = await analytics.flushActiveStays({ exitReason: "app_shutdown" });
  await analytics.flush();

  assert.deepEqual(shortResult, {
    accepted: false,
    reason: "stay_duration_below_minimum",
    durationMs: 300
  });
  assert.deepEqual(flushed, [{ key: "screen", accepted: true, eventId: "event-valid", durationMs: 3600 }]);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].properties.page, "checkout");
  assert.equal(sent[0].properties.exit_reason, "app_shutdown");
});

test("createSnowplowAdapter converts normalized events to self-describing payloads", async () => {
  const { createSnowplowAdapter } = require("../../snowplow-adapter/src/index");
  const payloads = [];
  const adapter = createSnowplowAdapter({
    tracker: {
      trackSelfDescribingEvent: async (payload) => {
        payloads.push(payload);
      }
    }
  });

  await adapter.send([
    {
      event_name: "add_to_cart",
      schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
      properties: { product_id: "sku-1" },
      context: { app: { app_id: "shop" } }
    }
  ]);

  assert.equal(payloads.length, 1);
  assert.deepEqual(payloads[0].event, {
    schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
    data: { product_id: "sku-1" }
  });
  assert.deepEqual(payloads[0].context[0].data, { app_id: "shop" });
});

function deterministicIds(values) {
  let index = 0;
  return () => values[index++] || `id-${index}`;
}

function deterministicClock(values) {
  let index = 0;
  return () => values[index++] || values[values.length - 1];
}

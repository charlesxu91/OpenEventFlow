const assert = require("node:assert/strict");
const test = require("node:test");

const { bindReactNativeAppStateStays, createReactNativeAnalytics } = require("../src/index");

test("react native app state stay binding excludes background and inactive time", async () => {
  let now = 1000;
  const sent = [];
  const appState = createFakeAppState("active");
  const analytics = createReactNativeAnalytics({
    endpoint: "https://collector.example/collect",
    fetch: async (_url, request) => {
      sent.push(...JSON.parse(request.body).events);
      return { ok: true };
    },
    appId: "shop",
    appVersion: "1.0.0",
    anonymousId: "anon-1",
    clock: () => now,
    idGenerator: createIdGenerator(["session-1", "stay-1", "event-1"])
  });

  const binding = bindReactNativeAppStateStays({
    analytics,
    appState,
    key: "product-detail",
    properties: { page: "product_detail" }
  });

  now = 1800;
  appState.emit("background");
  now = 5000;
  appState.emit("active");
  now = 5600;
  appState.emit("inactive");
  now = 6200;
  appState.emit("active");
  now = 7000;

  await binding.dispose({ exitReason: "test_shutdown" });
  await analytics.flush();

  assert.equal(sent.length, 1);
  assert.equal(sent[0].event_name, "page_stay");
  assert.equal(sent[0].properties.duration_ms, 2200);
  assert.equal(sent[0].properties.exit_reason, "test_shutdown");
});

function createFakeAppState(initialState) {
  const listeners = new Set();
  return {
    currentState: initialState,
    addEventListener(eventName, listener) {
      assert.equal(eventName, "change");
      listeners.add(listener);
      return {
        remove() {
          listeners.delete(listener);
        }
      };
    },
    emit(nextState) {
      this.currentState = nextState;
      for (const listener of listeners) {
        listener(nextState);
      }
    }
  };
}

function createIdGenerator(values) {
  let index = 0;
  return () => values[index++] || `id-${index}`;
}

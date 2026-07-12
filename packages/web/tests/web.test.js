const assert = require("node:assert/strict");
const test = require("node:test");
const { bindWebLifecycleStays, IndexedDBEventStore } = require("../src/index");

test("web lifecycle binding pauses on hidden, resumes on visible, and flushes on pagehide", async () => {
  const calls = [];
  const document = createTarget({ visibilityState: "visible" });
  const window = createTarget();
  const analytics = {
    pauseStay(key) {
      calls.push(["pause", key]);
    },
    resumeStay(key) {
      calls.push(["resume", key]);
    },
    async flushActiveStays(options) {
      calls.push(["flushActive", options]);
    },
    async flush() {
      calls.push(["flush"]);
    }
  };

  const binding = bindWebLifecycleStays({
    analytics,
    document,
    window,
    stayKeys: ["screen", "modal"]
  });

  document.visibilityState = "hidden";
  await document.dispatch("visibilitychange");
  document.visibilityState = "visible";
  await document.dispatch("visibilitychange");
  await window.dispatch("pagehide");
  binding.dispose();
  await window.dispatch("pagehide");

  assert.deepEqual(calls, [
    ["pause", "screen"],
    ["pause", "modal"],
    ["resume", "screen"],
    ["resume", "modal"],
    ["flushActive", { exitReason: "pagehide" }],
    ["flush"]
  ]);
});

test("IndexedDBEventStore preserves FIFO events and delegates atomic removal to its adapter", async () => {
  const records = [];
  const calls = [];
  const adapter = {
    async push(event) {
      calls.push(["push", event.event_id]);
      records.push(structuredClone(event));
    },
    async peek(limit) {
      calls.push(["peek", limit]);
      return records.slice(0, limit).map((record) => structuredClone(record));
    },
    async remove(count) {
      calls.push(["remove", count]);
      records.splice(0, count);
    },
    async size() {
      calls.push(["size"]);
      return records.length;
    },
    async close() {
      calls.push(["close"]);
    }
  };
  const store = new IndexedDBEventStore({ adapter });

  await store.push({ event_id: "evt-1" });
  await store.push({ event_id: "evt-2" });
  assert.deepEqual(await store.peek(1), [{ event_id: "evt-1" }]);
  await store.remove(1);
  assert.equal(await store.size(), 1);
  assert.deepEqual(await store.peek(10), [{ event_id: "evt-2" }]);
  await store.close();
  assert.deepEqual(calls, [
    ["push", "evt-1"], ["push", "evt-2"], ["peek", 1],
    ["remove", 1], ["size"], ["peek", 10], ["close"]
  ]);
});

test("IndexedDBEventStore validates queue operation bounds", async () => {
  const store = new IndexedDBEventStore({
    adapter: { push() {}, peek() {}, remove() {}, size() {} }
  });
  await assert.rejects(store.peek(-1), /non-negative integer/);
  await assert.rejects(store.remove(1.5), /non-negative integer/);
});

function createTarget(initial = {}) {
  const listeners = new Map();
  return {
    ...initial,
    addEventListener(name, handler) {
      if (!listeners.has(name)) {
        listeners.set(name, new Set());
      }
      listeners.get(name).add(handler);
    },
    removeEventListener(name, handler) {
      if (listeners.has(name)) {
        listeners.get(name).delete(handler);
      }
    },
    async dispatch(name) {
      for (const handler of listeners.get(name) || []) {
        await handler();
      }
    }
  };
}

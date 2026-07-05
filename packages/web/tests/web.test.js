const assert = require("node:assert/strict");
const test = require("node:test");
const { bindWebLifecycleStays } = require("../src/index");

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

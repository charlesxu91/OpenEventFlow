const assert = require("node:assert/strict");
const test = require("node:test");
const { createOpenEventFlowReact } = require("../src/index");

test("useStay begins stay on mount and ends stay on cleanup", async () => {
  const calls = [];
  const analytics = {
    beginStay(key, properties) {
      calls.push(["begin", key, properties]);
    },
    async endStay(key, options) {
      calls.push(["end", key, options]);
      return { accepted: true };
    }
  };
  let cleanup;
  const React = {
    createContext() {
      return { value: analytics };
    },
    createElement() {},
    useContext(context) {
      return context.value;
    },
    useEffect(effect) {
      cleanup = effect();
    }
  };
  const { useStay } = createOpenEventFlowReact(React);

  useStay("screen", { page: "product_detail" }, { exitReason: "component_unmount" });
  await cleanup();

  assert.deepEqual(calls, [
    ["begin", "screen", { page: "product_detail" }],
    ["end", "screen", { exitReason: "component_unmount" }]
  ]);
});

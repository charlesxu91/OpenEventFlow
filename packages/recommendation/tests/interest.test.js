import assert from "node:assert/strict";
import test from "node:test";

import { createInterestProfile } from "../src/index.js";

test("accumulates category, brand, price bucket, content type, and action signals", () => {
  const profile = createInterestProfile({ halfLifeMs: 1_000 });
  profile.apply({ timestamp: 0, action: "click", category: "sports", brand: "acme", price_bucket: "20-50", content_type: "video" });
  profile.apply({ timestamp: 0, action: "payment", category: "sports", brand: "acme", price_bucket: "20-50", content_type: "video", weight: 2 });

  assert.deepEqual(profile.snapshot(0), {
    at: 0,
    categories: { sports: 3 },
    brands: { acme: 3 },
    priceBuckets: { "20-50": 3 },
    contentTypes: { video: 3 },
    actions: { click: 1, payment: 2 }
  });
});

test("uses deterministic exponential half-life decay at snapshot time", () => {
  const profile = createInterestProfile({ halfLifeMs: 1_000 });
  profile.apply({ timestamp: 0, action: "click", category: "sports", weight: 2 });
  profile.apply({ timestamp: 1_000, action: "click", category: "news", weight: 1 });

  assert.deepEqual(profile.snapshot(2_000), {
    at: 2_000,
    categories: { news: 0.5, sports: 0.5 },
    brands: {},
    priceBuckets: {},
    contentTypes: {},
    actions: { click: 1 }
  });
});

test("rejects invalid clocks instead of depending on wall time", () => {
  assert.throws(() => createInterestProfile({ halfLifeMs: 0 }), /halfLifeMs/);
  const profile = createInterestProfile({ halfLifeMs: 1_000 });
  assert.throws(() => profile.apply({ timestamp: 10, action: "click" }).snapshot(9), /before/);
});

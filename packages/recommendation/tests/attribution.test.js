import assert from "node:assert/strict";
import test from "node:test";

import { createAttributionEngine } from "../src/index.js";

const impression = (overrides = {}) => ({
  event_id: "evt-impression",
  event_name: "recommendation_impression",
  timestamp: 1_000,
  user_id: "user-1",
  request_id: "request-1",
  impression_id: "impression-1",
  item_type: "video",
  item_id: "item-1",
  rank_position: 3,
  model_version: "model-v2",
  strategy_id: "strategy-home",
  experiment_id: "experiment-a",
  ...overrides
});

const action = (eventName, overrides = {}) => ({
  ...impression(),
  event_id: `evt-${eventName}`,
  event_name: eventName,
  timestamp: 2_000,
  ...overrides
});

function engine(options = {}) {
  return createAttributionEngine({
    attributionWindowMs: 10_000,
    allowedLatenessMs: 1_000,
    positiveActions: ["recommendation_click", "recommendation_add_to_cart", "recommendation_payment"],
    ...options
  });
}

test("deduplicates events by event_id", () => {
  const subject = engine();
  assert.deepEqual(subject.process(impression()).duplicates, []);

  const duplicate = subject.process(impression());

  assert.deepEqual(duplicate.samples, []);
  assert.deepEqual(duplicate.duplicates, [impression()]);
});

test("correlates click, cart, and payment labels with impression context", () => {
  const subject = engine();
  subject.process(impression());

  const samples = [
    ...subject.process(action("recommendation_click")).samples,
    ...subject.process(action("recommendation_add_to_cart")).samples,
    ...subject.process(action("recommendation_payment", { order_id: "order-1", payment_id: "pay-1" })).samples
  ];

  assert.deepEqual(samples.map(({ action: name, label }) => [name, label]), [
    ["recommendation_click", 1],
    ["recommendation_add_to_cart", 1],
    ["recommendation_payment", 1]
  ]);
  assert.deepEqual(samples[0], {
    sample_id: "impression-1:video:item-1:recommendation_click:evt-recommendation_click",
    label: 1,
    action: "recommendation_click",
    event_id: "evt-recommendation_click",
    event_time: 2_000,
    impression_event_id: "evt-impression",
    impression_time: 1_000,
    user_id: "user-1",
    request_id: "request-1",
    impression_id: "impression-1",
    item_type: "video",
    item_id: "item-1",
    rank_position: 3,
    model_version: "model-v2",
    strategy_id: "strategy-home",
    experiment_id: "experiment-a"
  });
});

test("requires impression id and item identity to correlate actions", () => {
  const subject = engine();
  subject.process(impression());

  assert.deepEqual(subject.process(action("recommendation_click", { item_id: "other" })).samples, []);
  assert.deepEqual(subject.process(action("recommendation_click", { event_id: "evt-other-type", item_type: "product" })).samples, []);
});

test("emits an expired negative sample only when no configured positive action occurred", () => {
  const subject = engine();
  subject.process(impression());

  const result = subject.advanceWatermark(12_001);

  assert.equal(result.samples.length, 1);
  assert.deepEqual(result.samples[0], {
    sample_id: "impression-1:video:item-1:negative:11000",
    label: 0,
    action: "negative",
    event_id: null,
    event_time: 11_000,
    impression_event_id: "evt-impression",
    impression_time: 1_000,
    user_id: "user-1",
    request_id: "request-1",
    impression_id: "impression-1",
    item_type: "video",
    item_id: "item-1",
    rank_position: 3,
    model_version: "model-v2",
    strategy_id: "strategy-home",
    experiment_id: "experiment-a"
  });

  const converted = engine();
  converted.process(impression());
  converted.process(action("recommendation_click"));
  assert.deepEqual(converted.advanceWatermark(12_001).samples, []);
});

test("emits refund corrections referencing the original payment sample", () => {
  const subject = engine();
  subject.process(impression());
  const payment = subject.process(action("recommendation_payment", { order_id: "order-1", payment_id: "pay-1" })).samples[0];

  const result = subject.process(action("recommendation_refund", {
    event_id: "evt-refund",
    timestamp: 3_000,
    order_id: "order-1",
    payment_id: "pay-1"
  }));

  assert.deepEqual(result.corrections, [{
    sample_id: "correction:evt-refund",
    correction: "refund",
    label: 0,
    event_id: "evt-refund",
    event_time: 3_000,
    original_sample_id: payment.sample_id,
    request_id: "request-1",
    impression_id: "impression-1",
    item_type: "video",
    item_id: "item-1",
    order_id: "order-1",
    payment_id: "pay-1"
  }]);
});

test("scopes same-order payment lookup to the full recommendation identity", () => {
  const subject = engine();
  subject.process(impression({ event_id: "imp-a", item_id: "item-a" }));
  subject.process(impression({ event_id: "imp-b", impression_id: "impression-2", item_id: "item-b" }));
  const paymentA = subject.process(action("recommendation_payment", {
    event_id: "pay-a",
    item_id: "item-a",
    order_id: "shared-order"
  })).samples[0];
  subject.process(action("recommendation_payment", {
    event_id: "pay-b",
    impression_id: "impression-2",
    item_id: "item-b",
    order_id: "shared-order"
  }));

  const correction = subject.process(action("recommendation_refund", {
    event_id: "refund-a",
    timestamp: 3_000,
    item_id: "item-a",
    order_id: "shared-order"
  })).corrections[0];

  assert.equal(correction.original_sample_id, paymentA.sample_id);
});

test("routes events older than allowed event-time lateness without mutating finalized state", () => {
  const subject = engine();
  subject.process(impression());
  subject.advanceWatermark(5_000);

  const late = action("recommendation_click", { timestamp: 3_999 });
  const result = subject.process(late);

  assert.deepEqual(result.samples, []);
  assert.deepEqual(result.lateEvents, [late]);

  const repeated = subject.process(late);
  assert.deepEqual(repeated.duplicates, []);
  assert.deepEqual(repeated.lateEvents, [late]);
});

test("rejects malformed base event fields", () => {
  const subject = engine();
  for (const [field, value] of [
    ["event_id", ""],
    ["event_name", null],
    ["timestamp", Number.NaN]
  ]) {
    assert.throws(() => subject.process(impression({ [field]: value })), new RegExp(field));
  }
});

test("rejects events missing the required recommendation correlation tuple", () => {
  for (const field of ["request_id", "impression_id", "item_type", "item_id"]) {
    const subject = engine();
    assert.throws(() => subject.process(impression({ [field]: undefined })), new RegExp(field));
  }
});

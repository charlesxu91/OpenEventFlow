const emptyOutput = () => ({ samples: [], corrections: [], lateEvents: [], duplicates: [] });

const identityKey = ({ request_id, impression_id, item_type, item_id }) =>
  `${request_id}\u0000${impression_id}\u0000${item_type}\u0000${item_id}`;

const requiredStrings = ["event_id", "event_name", "request_id", "impression_id", "item_type", "item_id"];

function validateEvent(event) {
  if (!event || typeof event !== "object") throw new TypeError("event must be an object");
  for (const field of requiredStrings) {
    if (typeof event[field] !== "string" || event[field].length === 0) {
      throw new TypeError(`${field} must be a non-empty string`);
    }
  }
  if (!Number.isFinite(event.timestamp)) throw new TypeError("timestamp must be a finite number");
}

const sampleContext = (impression) => ({
  impression_event_id: impression.event_id,
  impression_time: impression.timestamp,
  user_id: impression.user_id,
  request_id: impression.request_id,
  impression_id: impression.impression_id,
  item_type: impression.item_type,
  item_id: impression.item_id,
  rank_position: impression.rank_position,
  model_version: impression.model_version,
  strategy_id: impression.strategy_id,
  experiment_id: impression.experiment_id
});

const conversionKeys = (event) => [
  event.payment_id && `${identityKey(event)}\u0000payment:${event.payment_id}`,
  event.order_id && `${identityKey(event)}\u0000order:${event.order_id}`,
  `${identityKey(event)}\u0000identity`
].filter(Boolean);

export function createAttributionEngine({ attributionWindowMs, allowedLatenessMs, positiveActions }) {
  if (!Number.isFinite(attributionWindowMs) || attributionWindowMs < 0) {
    throw new TypeError("attributionWindowMs must be a non-negative finite number");
  }
  if (!Number.isFinite(allowedLatenessMs) || allowedLatenessMs < 0) {
    throw new TypeError("allowedLatenessMs must be a non-negative finite number");
  }
  if (!Array.isArray(positiveActions)) {
    throw new TypeError("positiveActions must be an array");
  }

  const positive = new Set(positiveActions);
  const seenEventIds = new Set();
  const impressions = new Map();
  const conversions = new Map();
  let watermark = Number.NEGATIVE_INFINITY;

  function process(event) {
    const output = emptyOutput();
    validateEvent(event);
    if (event.timestamp < watermark - allowedLatenessMs) {
      output.lateEvents.push(event);
      return output;
    }
    if (seenEventIds.has(event.event_id)) {
      output.duplicates.push(event);
      return output;
    }
    seenEventIds.add(event.event_id);

    if (event.event_name === "recommendation_impression") {
      impressions.set(identityKey(event), { event, converted: false });
      return output;
    }

    if (event.event_name === "refund" || event.event_name === "recommendation_refund") {
      const original = conversionKeys(event).map((key) => conversions.get(key)).find(Boolean);
      if (original) {
        output.corrections.push({
          sample_id: `correction:${event.event_id}`,
          correction: "refund",
          label: 0,
          event_id: event.event_id,
          event_time: event.timestamp,
          original_sample_id: original.sample_id,
          request_id: event.request_id,
          impression_id: event.impression_id,
          item_type: event.item_type,
          item_id: event.item_id,
          order_id: event.order_id,
          payment_id: event.payment_id
        });
      }
      return output;
    }

    if (!positive.has(event.event_name)) return output;
    const state = impressions.get(identityKey(event));
    if (!state || event.timestamp < state.event.timestamp ||
        event.timestamp > state.event.timestamp + attributionWindowMs) {
      return output;
    }

    state.converted = true;
    const sample = {
      sample_id: `${event.impression_id}:${event.item_type}:${event.item_id}:${event.event_name}:${event.event_id}`,
      label: 1,
      action: event.event_name,
      event_id: event.event_id,
      event_time: event.timestamp,
      ...sampleContext(state.event)
    };
    output.samples.push(sample);
    if (event.event_name === "payment" || event.event_name === "recommendation_payment") {
      for (const key of conversionKeys(event)) conversions.set(key, sample);
    }
    return output;
  }

  function advanceWatermark(timestamp) {
    if (!Number.isFinite(timestamp) || timestamp < watermark) {
      throw new RangeError("watermark must be a finite, non-decreasing timestamp");
    }
    watermark = timestamp;
    const output = emptyOutput();
    for (const [key, state] of impressions) {
      const expiresAt = state.event.timestamp + attributionWindowMs;
      if (watermark <= expiresAt + allowedLatenessMs) continue;
      if (!state.converted) {
        output.samples.push({
          sample_id: `${state.event.impression_id}:${state.event.item_type}:${state.event.item_id}:negative:${expiresAt}`,
          label: 0,
          action: "negative",
          event_id: null,
          event_time: expiresAt,
          ...sampleContext(state.event)
        });
      }
      impressions.delete(key);
    }
    return output;
  }

  return { process, advanceWatermark };
}

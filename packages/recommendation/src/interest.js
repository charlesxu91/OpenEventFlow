const signalDimensions = [
  ["category", "categories"],
  ["brand", "brands"],
  ["price_bucket", "priceBuckets"],
  ["content_type", "contentTypes"],
  ["action", "actions"]
];

export function createInterestProfile({ halfLifeMs }) {
  if (!Number.isFinite(halfLifeMs) || halfLifeMs <= 0) {
    throw new TypeError("halfLifeMs must be a positive finite number");
  }

  const signals = [];
  let latestTimestamp = Number.NEGATIVE_INFINITY;

  const profile = {
    apply(event) {
      if (!Number.isFinite(event.timestamp)) {
        throw new TypeError("event timestamp must be finite");
      }
      if (typeof event.action !== "string" || event.action.length === 0) {
        throw new TypeError("event action must be a non-empty string");
      }
      const weight = event.weight ?? 1;
      if (!Number.isFinite(weight)) throw new TypeError("event weight must be finite");
      signals.push({ event, weight });
      latestTimestamp = Math.max(latestTimestamp, event.timestamp);
      return profile;
    },

    snapshot(at) {
      if (!Number.isFinite(at)) throw new TypeError("snapshot time must be finite");
      if (at < latestTimestamp) throw new RangeError("snapshot time cannot be before an applied event");

      const result = {
        at,
        categories: {},
        brands: {},
        priceBuckets: {},
        contentTypes: {},
        actions: {}
      };
      for (const { event, weight } of signals) {
        const decayed = weight * Math.pow(2, -(at - event.timestamp) / halfLifeMs);
        for (const [input, output] of signalDimensions) {
          const value = event[input];
          if (value === undefined || value === null || value === "") continue;
          result[output][value] = (result[output][value] ?? 0) + decayed;
        }
      }
      return result;
    }
  };

  return profile;
}

function createSnowplowAdapter(options) {
  if (!options || !options.tracker) {
    throw new Error("tracker is required");
  }

  return {
    async send(batch) {
      for (const event of batch) {
        await sendOne(options.tracker, event);
      }
      return { ok: true };
    }
  };
}

async function sendOne(tracker, event) {
  const payload = {
    event: {
      schema: event.schema,
      data: event.properties || {}
    },
    context: contextsFrom(event.context)
  };

  if (typeof tracker.trackSelfDescribingEvent === "function") {
    return tracker.trackSelfDescribingEvent(payload);
  }
  if (typeof tracker.track === "function") {
    return tracker.track(payload);
  }
  throw new Error("tracker must expose trackSelfDescribingEvent or track");
}

function contextsFrom(context = {}) {
  return Object.entries(context).map(([name, data]) => ({
    schema: `iglu:io.openeventflow/${name}_context/jsonschema/1-0-0`,
    data
  }));
}

module.exports = {
  createSnowplowAdapter,
  contextsFrom
};

const { MemoryEventStore, createAnalytics } = require("../../core/src/index");

function createReactNativeAnalytics(options) {
  const analyticsOptions = {
    store: options.store || new MemoryEventStore(),
    transport: options.transport || createReactNativeTransport(options.endpoint, options.fetch || globalThis.fetch),
    app: {
      appId: options.appId,
      platform: options.platform || "react-native",
      appVersion: options.appVersion || "0.0.0",
      sdkVersion: "0.1.0"
    },
    anonymousId: options.anonymousId
  };
  for (const key of ["clock", "consent", "idGenerator", "stay"]) {
    if (options[key] !== undefined) {
      analyticsOptions[key] = options[key];
    }
  }
  return createAnalytics(analyticsOptions);
}

function createReactNativeTransport(endpoint, fetchImpl) {
  if (!endpoint) {
    throw new Error("endpoint is required");
  }
  return {
    async send(batch) {
      const response = await fetchImpl(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ events: batch })
      });
      if (!response.ok) {
        throw new Error(`analytics upload failed: ${response.status}`);
      }
      return { ok: true };
    }
  };
}

function bindReactNativeAppStateStays({ analytics, appState, key, properties = {} }) {
  if (!analytics || typeof analytics.beginStay !== "function") {
    throw new Error("analytics stay API is required");
  }
  if (!appState || typeof appState.addEventListener !== "function") {
    throw new Error("appState.addEventListener is required");
  }

  analytics.beginStay(key, properties);
  const subscription = appState.addEventListener("change", (nextState) => {
    if (nextState === "active") {
      analytics.resumeStay(key);
    } else if (nextState === "background" || nextState === "inactive") {
      analytics.pauseStay(key);
    }
  });

  return {
    async dispose(options = {}) {
      if (subscription && typeof subscription.remove === "function") {
        subscription.remove();
      }
      return analytics.endStay(key, {
        exitReason: options.exitReason || "app_state_dispose",
        properties: options.properties
      });
    }
  };
}

module.exports = {
  bindReactNativeAppStateStays,
  createReactNativeAnalytics,
  createReactNativeTransport
};

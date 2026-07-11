const { MemoryEventStore, createAnalytics } = require("../../core/src/index");
const { IndexedDBEventStore, createIndexedDBAdapter } = require("./indexeddb-event-store");

function createFetchTransport(endpoint, fetchImpl = globalThis.fetch) {
  if (!endpoint) {
    throw new Error("endpoint is required");
  }
  if (typeof fetchImpl !== "function") {
    throw new Error("fetch implementation is required");
  }
  return {
    async send(batch) {
      const response = await fetchImpl(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ events: batch }),
        keepalive: true
      });
      if (!response.ok) {
        throw new Error(`analytics upload failed: ${response.status}`);
      }
      return { ok: true };
    }
  };
}

function createWebAnalytics(options) {
  const analytics = createAnalytics({
    store: options.store || new MemoryEventStore(),
    transport: options.transport || createFetchTransport(options.endpoint, options.fetch),
    app: {
      appId: options.appId,
      platform: "web",
      appVersion: options.appVersion || "0.0.0",
      sdkVersion: "0.1.0"
    },
    anonymousId: options.anonymousId
  });

  if (options.autotrack && options.autotrack.screen !== false && typeof window !== "undefined") {
    analytics.screen(document.title || window.location.pathname, {
      path: window.location.pathname,
      url: window.location.href
    });
  }

  if (options.autotrack && options.autotrack.stay && typeof window !== "undefined") {
    const key = options.autotrack.stay.key || "screen";
    analytics.beginStay(key, {
      page: options.autotrack.stay.page || window.location.pathname,
      url: window.location.href
    });
    bindWebLifecycleStays({
      analytics,
      document,
      window,
      stayKeys: [key]
    });
  }

  return analytics;
}

function bindClickAutotrack(root, analytics, options = {}) {
  const eventName = options.eventName || "element_click";
  const schema = options.schema || "iglu:io.openeventflow/element_click/jsonschema/1-0-0";
  root.addEventListener("click", (event) => {
    const target = event.target && event.target.closest ? event.target.closest("[data-ob-track]") : null;
    if (!target) {
      return;
    }
    analytics.track({
      name: eventName,
      schema,
      properties: {
        element_id: target.getAttribute("data-ob-track"),
        text: (target.textContent || "").trim().slice(0, 120)
      }
    });
  });
}

function bindWebLifecycleStays({ analytics, document, window, stayKeys = ["screen"] }) {
  const pauseAll = () => {
    for (const key of stayKeys) {
      analytics.pauseStay(key);
    }
  };
  const resumeAll = () => {
    for (const key of stayKeys) {
      analytics.resumeStay(key);
    }
  };
  const onVisibilityChange = () => {
    if (document.visibilityState === "hidden") {
      pauseAll();
    } else {
      resumeAll();
    }
  };
  const onPageHide = async () => {
    await analytics.flushActiveStays({ exitReason: "pagehide" });
    await analytics.flush();
  };

  document.addEventListener("visibilitychange", onVisibilityChange);
  window.addEventListener("pagehide", onPageHide);

  return {
    dispose() {
      document.removeEventListener("visibilitychange", onVisibilityChange);
      window.removeEventListener("pagehide", onPageHide);
    }
  };
}

module.exports = {
  IndexedDBEventStore,
  bindWebLifecycleStays,
  bindClickAutotrack,
  createFetchTransport,
  createWebAnalytics,
  createIndexedDBAdapter
};

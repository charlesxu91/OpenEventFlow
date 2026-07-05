class MemoryEventStore {
  constructor() {
    this.events = [];
  }

  async push(event) {
    this.events.push(event);
  }

  async peek(limit) {
    return this.events.slice(0, limit);
  }

  async remove(count) {
    this.events.splice(0, count);
  }

  async size() {
    return this.events.length;
  }
}

function createAnalytics(options) {
  const config = {
    batchSize: 50,
    consent: { analyticsAllowed: true, advertisingAllowed: false, piiAllowed: false },
    idGenerator: defaultIdGenerator,
    clock: () => Date.now(),
    stay: {},
    ...options
  };
  const store = config.store || new MemoryEventStore();
  const identity = {
    anonymous_id: config.anonymousId || defaultIdGenerator(),
    user_id: null,
    traits: {}
  };
  let consent = { ...config.consent };
  let session = createSession(config.clock);
  const stays = new Map();
  const stayConfig = {
    minDurationMs: 1000,
    maxDurationMs: 24 * 60 * 60 * 1000,
    schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
    ...config.stay
  };

  if (!config.transport || typeof config.transport.send !== "function") {
    throw new Error("transport.send is required");
  }
  if (!config.app) {
    throw new Error("app context is required");
  }

  async function track(event) {
    if (!consent.analyticsAllowed) {
      return { accepted: false, reason: "analytics_consent_disabled" };
    }
    validateEvent(event);
    const normalized = normalizeEvent(event, {
      app: config.app,
      clock: config.clock,
      consent,
      idGenerator: config.idGenerator,
      identity,
      session
    });
    await store.push(normalized);
    return { accepted: true, eventId: normalized.event_id };
  }

  async function screen(name, properties = {}) {
    return track({
      name: "screen_view",
      schema: config.screenSchema || "iglu:io.openeventflow/screen_view/jsonschema/1-0-0",
      properties: { name, ...properties }
    });
  }

  function beginStay(key, properties = {}) {
    if (!key) {
      throw new Error("stay key is required");
    }
    const startedAt = config.clock();
    const stay = {
      key,
      stay_id: config.idGenerator(),
      properties: { ...properties },
      started_at: startedAt,
      active_started_at: startedAt,
      accumulated_ms: 0,
      paused: false
    };
    stays.set(key, stay);
    return { accepted: true, stayId: stay.stay_id, startedAt };
  }

  function pauseStay(key) {
    const stay = stays.get(key);
    if (!stay) {
      return { accepted: false, reason: "stay_not_found" };
    }
    if (stay.paused) {
      return { accepted: true, durationMs: currentStayDuration(stay, config.clock(), stayConfig.maxDurationMs) };
    }
    const now = config.clock();
    stay.accumulated_ms += now - stay.active_started_at;
    stay.paused = true;
    stay.paused_at = now;
    return { accepted: true, durationMs: clampDuration(stay.accumulated_ms, stayConfig.maxDurationMs) };
  }

  function resumeStay(key) {
    const stay = stays.get(key);
    if (!stay) {
      return { accepted: false, reason: "stay_not_found" };
    }
    if (!stay.paused) {
      return { accepted: true, durationMs: currentStayDuration(stay, config.clock(), stayConfig.maxDurationMs) };
    }
    stay.active_started_at = config.clock();
    stay.paused = false;
    delete stay.paused_at;
    return { accepted: true, durationMs: clampDuration(stay.accumulated_ms, stayConfig.maxDurationMs) };
  }

  async function endStay(key, options = {}) {
    const stay = stays.get(key);
    if (!stay) {
      return { accepted: false, reason: "stay_not_found" };
    }
    const now = config.clock();
    const durationMs = currentStayDuration(stay, now, stayConfig.maxDurationMs);
    stays.delete(key);
    if (durationMs < stayConfig.minDurationMs) {
      return {
        accepted: false,
        reason: "stay_duration_below_minimum",
        durationMs
      };
    }
    const result = await track({
      name: "page_stay",
      schema: stayConfig.schema,
      clientTime: now,
      properties: {
        ...stay.properties,
        ...(options.properties || {}),
        stay_id: stay.stay_id,
        duration_ms: durationMs,
        exit_reason: options.exitReason || "unknown"
      }
    });
    return { ...result, durationMs };
  }

  async function switchStay(key, properties = {}, options = {}) {
    const ended = stays.has(key)
      ? await endStay(key, { exitReason: options.exitReason || "route_change" })
      : { accepted: false, reason: "stay_not_found" };
    beginStay(key, properties);
    return ended;
  }

  function cancelStay(key) {
    const existed = stays.delete(key);
    return { accepted: existed };
  }

  async function flushActiveStays(options = {}) {
    const keys = Array.from(stays.keys());
    const results = [];
    for (const key of keys) {
      const result = await endStay(key, options);
      results.push({ key, ...result });
    }
    return results;
  }

  function identify(userId, traits = {}) {
    identity.user_id = userId;
    identity.traits = { ...traits };
  }

  function setConsent(nextConsent) {
    consent = { ...consent, ...nextConsent };
  }

  async function flush() {
    const batch = await store.peek(config.batchSize);
    if (batch.length === 0) {
      return { sent: 0, remaining: 0 };
    }
    await config.transport.send(batch);
    await store.remove(batch.length);
    return { sent: batch.length, remaining: await store.size() };
  }

  async function queueSize() {
    return store.size();
  }

  function startNewSession() {
    session = createSession(config.clock);
    return session.session_id;
  }

  return {
    beginStay,
    cancelStay,
    endStay,
    flush,
    flushActiveStays,
    identify,
    pauseStay,
    queueSize,
    resumeStay,
    screen,
    setConsent,
    startNewSession,
    switchStay,
    track
  };
}

function normalizeEvent(event, context) {
  return {
    event_id: context.idGenerator(),
    event_name: event.name,
    schema: event.schema,
    client_time: event.clientTime === undefined ? context.clock() : event.clientTime,
    properties: sanitizeProperties(event.properties || {}, context.consent),
    context: {
      app: normalizeAppContext(context.app),
      user: {
        anonymous_id: context.identity.anonymous_id,
        user_id: context.identity.user_id,
        traits: context.consent.piiAllowed ? context.identity.traits : {}
      },
      session: context.session,
      privacy: context.consent
    }
  };
}

function normalizeAppContext(app) {
  return {
    app_id: app.appId,
    platform: app.platform,
    app_version: app.appVersion,
    sdk_version: app.sdkVersion
  };
}

function sanitizeProperties(properties, consent) {
  const result = {};
  for (const [key, value] of Object.entries(properties)) {
    if (!consent.piiAllowed && isLikelyPiiKey(key)) {
      continue;
    }
    result[key] = value;
  }
  return result;
}

function isLikelyPiiKey(key) {
  return /email|phone|mobile|id_card|password|address/i.test(key);
}

function validateEvent(event) {
  if (!event || typeof event !== "object") {
    throw new Error("event must be an object");
  }
  if (!event.name) {
    throw new Error("event.name is required");
  }
  if (!event.schema) {
    throw new Error("event.schema is required");
  }
}

function createSession(clock) {
  return {
    session_id: defaultIdGenerator(),
    started_at: clock()
  };
}

function currentStayDuration(stay, now, maxDurationMs) {
  const activeDuration = stay.paused ? 0 : now - stay.active_started_at;
  return Math.max(0, clampDuration(stay.accumulated_ms + activeDuration, maxDurationMs));
}

function clampDuration(durationMs, maxDurationMs) {
  return Math.min(Math.max(0, durationMs), maxDurationMs);
}

function defaultIdGenerator() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `evt_${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
}

module.exports = {
  MemoryEventStore,
  createAnalytics
};

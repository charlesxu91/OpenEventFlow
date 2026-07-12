const http = require("node:http");
const crypto = require("node:crypto");

function createInMemoryTopicBroker() {
  const topics = new Map();
  return {
    publish(topic, message) {
      if (!topics.has(topic)) topics.set(topic, []);
      topics.get(topic).push(message);
    },
    async publishBatch(records) {
      for (const record of records) this.publish(record.topic, record.message);
    },
    async health() { return { ready: true }; },
    async close() {},
    topic(name) { return topics.get(name) || []; }
  };
}

function createTrackingPlanRegistry(trackingPlan) {
  const schemas = new Map();
  for (const event of trackingPlan.events || []) {
    schemas.set(`iglu:${trackingPlan.schemaVendor}/${event.name}/jsonschema/${event.version}`, event);
  }
  return {
    validate(event) {
      const definition = schemas.get(event.schema);
      if (!definition) return { valid: false, reason: "schema_not_found" };
      if (definition.name !== event.event_name) return { valid: false, reason: "event_name_schema_mismatch" };
      const properties = event.properties || {};
      for (const requiredProperty of definition.required || []) {
        if (properties[requiredProperty] === undefined || properties[requiredProperty] === null) {
          return { valid: false, reason: "missing_required_property", property: requiredProperty };
        }
      }
      for (const [propertyName, value] of Object.entries(properties)) {
        const propertyDefinition = definition.properties && definition.properties[propertyName];
        if (!propertyDefinition) return { valid: false, reason: "unknown_property", property: propertyName };
        if (!matchesType(value, propertyDefinition.type)) {
          return {
            valid: false,
            reason: "invalid_property_type",
            property: propertyName,
            expected: propertyDefinition.type,
            actual: Array.isArray(value) ? "array" : typeof value
          };
        }
      }
      return { valid: true };
    }
  };
}

function createCollector(options) {
  const config = {
    rawTopic: "snowplow_raw_events",
    validTopic: "snowplow_enriched_events",
    badTopic: "snowplow_bad_events",
    clock: () => Date.now(),
    enrich: defaultEnrich,
    ...options
  };
  if (!config.broker || typeof config.broker.publish !== "function") throw new Error("broker.publish is required");
  if (!config.registry || typeof config.registry.validate !== "function") throw new Error("registry.validate is required");

  let closed = false;
  return {
    async collect(events) {
      if (closed) throw new ServiceUnavailableError("collector is closed");
      if (!Array.isArray(events)) throw new Error("events must be an array");
      let enriched = 0;
      let bad = 0;
      const records = [];
      for (const event of events) {
        records.push({ topic: config.rawTopic, message: event, key: event && event.event_id });
        const validation = config.registry.validate(event);
        if (validation.valid) {
          records.push({ topic: config.validTopic, message: config.enrich(event, config.clock), key: event && event.event_id });
          enriched += 1;
        } else {
          records.push({
            topic: config.badTopic,
            message: { ...validation, event, collector_time: config.clock() },
            key: event && event.event_id
          });
          bad += 1;
        }
      }
      try {
        if (typeof config.broker.publishBatch === "function") await config.broker.publishBatch(records);
        else await Promise.all(records.map((record) => config.broker.publish(record.topic, record.message, record.key)));
      } catch (cause) {
        const error = new ServiceUnavailableError("failed to publish collector batch");
        error.cause = cause;
        throw error;
      }
      return { accepted: events.length, enriched, bad };
    },
    async health() {
      if (closed) return { ready: false, reason: "collector_closed" };
      return typeof config.broker.health === "function" ? config.broker.health() : { ready: true };
    },
    async close() {
      if (closed) return;
      closed = true;
      if (typeof config.broker.close === "function") await config.broker.close();
    }
  };
}

function createHttpCollectorServer({
  collector,
  path = "/collect",
  healthPath = "/health/live",
  readinessPath = "/health/ready",
  maxBodyBytes = 1024 * 1024,
  maxBatchSize = 500,
  apiKeys = [],
  apiKey,
  readiness
}) {
  if (!collector || typeof collector.collect !== "function") throw new Error("collector.collect is required");
  const configuredApiKeys = apiKey === undefined ? apiKeys : [...apiKeys, apiKey];
  return http.createServer(async (request, response) => {
    const requestPath = request.url.split("?")[0];
    if (request.method === "GET" && requestPath === healthPath) {
      sendJson(response, 200, { status: "ok" });
      return;
    }
    if (request.method === "GET" && requestPath === readinessPath) {
      try {
        const health = readiness
          ? { ready: await readiness() }
          : typeof collector.health === "function" ? await collector.health() : { ready: true };
        sendJson(response, health.ready === false ? 503 : 200, health);
      } catch (error) {
        sendJson(response, 503, { ready: false, reason: error.message });
      }
      return;
    }
    if (request.method !== "POST" || requestPath !== path) {
      sendJson(response, 404, { error: "not_found" });
      return;
    }
    try {
      if (configuredApiKeys.length > 0 && !hasValidApiKey(request, configuredApiKeys)) {
        throw new HttpError(401, "unauthorized", "a valid collector API key is required");
      }
      const contentType = String(request.headers["content-type"] || "").split(";", 1)[0];
      if (contentType !== "application/json") {
        throw new HttpError(415, "unsupported_media_type", "content-type must be application/json");
      }
      const payload = JSON.parse(await readRequestBody(request, maxBodyBytes));
      const events = Array.isArray(payload) ? payload : payload.events;
      if (!Array.isArray(events)) throw new HttpError(400, "invalid_batch", "payload must be an event array or contain events");
      if (events.length === 0 || events.length > maxBatchSize) {
        throw new HttpError(413, "batch_too_large", `batch must contain between 1 and ${maxBatchSize} events`);
      }
      sendJson(response, 202, await collector.collect(events));
    } catch (error) {
      const statusCode = error.statusCode || (error instanceof SyntaxError ? 400 : 503);
      sendJson(response, statusCode, {
        error: error.code || (error instanceof SyntaxError ? "invalid_json" : "collector_unavailable"),
        message: error.message
      });
    }
  });
}

function hasValidApiKey(request, apiKeys) {
  const authorization = String(request.headers.authorization || "");
  const supplied = String(request.headers["x-api-key"] || (authorization.startsWith("Bearer ") ? authorization.slice(7) : ""));
  if (!supplied) return false;
  const suppliedDigest = crypto.createHash("sha256").update(supplied).digest();
  return apiKeys.some((key) => crypto.timingSafeEqual(suppliedDigest, crypto.createHash("sha256").update(String(key)).digest()));
}

function defaultEnrich(event, clock) {
  const collectorTime = clock();
  return { ...event, collector_time: collectorTime, enriched_at: collectorTime };
}

function matchesType(value, expectedType) {
  if (expectedType === "integer") return Number.isInteger(value);
  if (expectedType === "number") return typeof value === "number" && Number.isFinite(value);
  if (expectedType === "object") return value !== null && typeof value === "object" && !Array.isArray(value);
  if (expectedType === "array") return Array.isArray(value);
  return typeof value === expectedType;
}

function readRequestBody(request, maxBodyBytes) {
  return new Promise((resolve, reject) => {
    let body = "";
    let bytes = 0;
    let rejected = false;
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      if (rejected) return;
      bytes += Buffer.byteLength(chunk);
      if (bytes > maxBodyBytes) {
        rejected = true;
        reject(new HttpError(413, "body_too_large", `request body exceeds ${maxBodyBytes} bytes`));
        return;
      }
      body += chunk;
    });
    request.on("end", () => { if (!rejected) resolve(body || "{}"); });
    request.on("error", (error) => { if (!rejected) reject(error); });
  });
}

class HttpError extends Error {
  constructor(statusCode, code, message) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
  }
}

class ServiceUnavailableError extends HttpError {
  constructor(message) { super(503, "collector_unavailable", message); }
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "content-type": "application/json" });
  response.end(JSON.stringify(payload));
}

module.exports = {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry,
  ...require("./kafka-broker")
};

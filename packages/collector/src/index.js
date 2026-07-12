const http = require("node:http");
const { timingSafeEqual } = require("node:crypto");

function createInMemoryTopicBroker() {
  const topics = new Map();
  return {
    publish(topic, message) {
      if (!topics.has(topic)) {
        topics.set(topic, []);
      }
      topics.get(topic).push(message);
    },
    topic(name) {
      return topics.get(name) || [];
    }
  };
}

function createTrackingPlanRegistry(trackingPlan) {
  const schemas = new Map();
  for (const event of trackingPlan.events || []) {
    const schema = `iglu:${trackingPlan.schemaVendor}/${event.name}/jsonschema/${event.version}`;
    schemas.set(schema, event);
  }
  return {
    validate(event) {
      const definition = schemas.get(event.schema);
      if (!definition) {
        return { valid: false, reason: "schema_not_found" };
      }
      if (definition.name !== event.event_name) {
        return { valid: false, reason: "event_name_schema_mismatch" };
      }

      const properties = event.properties || {};
      for (const requiredProperty of definition.required || []) {
        if (properties[requiredProperty] === undefined || properties[requiredProperty] === null) {
          return {
            valid: false,
            reason: "missing_required_property",
            property: requiredProperty
          };
        }
      }

      for (const [propertyName, value] of Object.entries(properties)) {
        const propertyDefinition = definition.properties && definition.properties[propertyName];
        if (!propertyDefinition) {
          return {
            valid: false,
            reason: "unknown_property",
            property: propertyName
          };
        }
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
  if (!config.broker || typeof config.broker.publish !== "function") {
    throw new Error("broker.publish is required");
  }
  if (!config.registry || typeof config.registry.validate !== "function") {
    throw new Error("registry.validate is required");
  }

  return {
    async collect(events) {
      if (!Array.isArray(events)) {
        throw new Error("events must be an array");
      }

      let enriched = 0;
      let bad = 0;
      for (const event of events) {
        await publish(config.broker, config.rawTopic, event);
        const validation = config.registry.validate(event);
        if (validation.valid) {
          await publish(config.broker, config.validTopic, config.enrich(event, config.clock));
          enriched += 1;
        } else {
          await publish(config.broker, config.badTopic, {
            ...validation,
            event,
            collector_time: config.clock()
          });
          bad += 1;
        }
      }

      return { accepted: events.length, enriched, bad };
    }
  };
}

function createHttpCollectorServer({
  collector,
  path = "/collect",
  apiKey,
  maxBodyBytes = 1024 * 1024,
  readiness = async () => true
}) {
  if (!collector || typeof collector.collect !== "function") {
    throw new Error("collector.collect is required");
  }

  return http.createServer(async (request, response) => {
    const requestPath = request.url.split("?")[0];
    if (request.method === "GET" && requestPath === "/healthz") {
      sendJson(response, 200, { status: "ok" });
      return;
    }
    if (request.method === "GET" && requestPath === "/readyz") {
      let ready = false;
      try {
        ready = await readiness();
      } catch {
        ready = false;
      }
      sendJson(response, ready ? 200 : 503, { status: ready ? "ready" : "not_ready" });
      return;
    }
    if (request.method !== "POST" || requestPath !== path) {
      sendJson(response, 404, { error: "not_found" });
      return;
    }

    if (apiKey && !safeEqual(request.headers["x-api-key"], apiKey)) {
      sendJson(response, 401, { error: "unauthorized" });
      return;
    }

    try {
      const payload = JSON.parse(await readRequestBody(request, maxBodyBytes));
      const events = Array.isArray(payload) ? payload : payload.events;
      const result = await collector.collect(events);
      sendJson(response, 202, result);
    } catch (error) {
      const statusCode = error.code === "PAYLOAD_TOO_LARGE" ? 413
        : error.code === "BROKER_UNAVAILABLE" ? 503
          : 400;
      sendJson(response, statusCode, {
        error: statusCode === 413 ? "payload_too_large"
          : statusCode === 503 ? "service_unavailable"
            : "bad_request",
        message: error.message
      });
    }
  });
}

function defaultEnrich(event, clock) {
  const collectorTime = clock();
  return {
    ...event,
    collector_time: collectorTime,
    enriched_at: collectorTime
  };
}

function matchesType(value, expectedType) {
  if (expectedType === "integer") {
    return Number.isInteger(value);
  }
  if (expectedType === "number") {
    return typeof value === "number" && Number.isFinite(value);
  }
  if (expectedType === "object") {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }
  if (expectedType === "array") {
    return Array.isArray(value);
  }
  return typeof value === expectedType;
}

function readRequestBody(request, maxBodyBytes) {
  return new Promise((resolve, reject) => {
    let body = "";
    let bytes = 0;
    let settled = false;
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      if (settled) return;
      bytes += Buffer.byteLength(chunk);
      if (bytes > maxBodyBytes) {
        settled = true;
        const error = new Error(`request body exceeds ${maxBodyBytes} bytes`);
        error.code = "PAYLOAD_TOO_LARGE";
        reject(error);
        return;
      }
      body += chunk;
    });
    request.on("end", () => {
      if (!settled) resolve(body || "{}");
    });
    request.on("error", (error) => {
      if (!settled) reject(error);
    });
  });
}

async function publish(broker, topic, message) {
  try {
    await broker.publish(topic, message);
  } catch (cause) {
    const error = new Error(`failed to publish to ${topic}`);
    error.code = "BROKER_UNAVAILABLE";
    error.cause = cause;
    throw error;
  }
}

function safeEqual(candidate, expected) {
  if (typeof candidate !== "string") return false;
  const left = Buffer.from(candidate);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "content-type": "application/json" });
  response.end(JSON.stringify(payload));
}

module.exports = {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry
};

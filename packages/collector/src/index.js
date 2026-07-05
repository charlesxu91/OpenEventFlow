const http = require("node:http");

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
        config.broker.publish(config.rawTopic, event);
        const validation = config.registry.validate(event);
        if (validation.valid) {
          config.broker.publish(config.validTopic, config.enrich(event, config.clock));
          enriched += 1;
        } else {
          config.broker.publish(config.badTopic, {
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

function createHttpCollectorServer({ collector, path = "/collect" }) {
  if (!collector || typeof collector.collect !== "function") {
    throw new Error("collector.collect is required");
  }

  return http.createServer(async (request, response) => {
    if (request.method !== "POST" || request.url.split("?")[0] !== path) {
      sendJson(response, 404, { error: "not_found" });
      return;
    }

    try {
      const payload = JSON.parse(await readRequestBody(request));
      const events = Array.isArray(payload) ? payload : payload.events;
      const result = await collector.collect(events);
      sendJson(response, 202, result);
    } catch (error) {
      sendJson(response, 400, {
        error: "bad_request",
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

function readRequestBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => resolve(body || "{}"));
    request.on("error", reject);
  });
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

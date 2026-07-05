(function attachOpenEventFlow(global) {
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

  class InMemoryTopicBroker {
    constructor() {
      this.topics = new Map();
    }

    publish(topic, message) {
      if (!this.topics.has(topic)) {
        this.topics.set(topic, []);
      }
      this.topics.get(topic).push(message);
    }

    topic(name) {
      return this.topics.get(name) || [];
    }
  }

  class TrackingPlanRegistry {
    constructor(trackingPlan) {
      this.schemas = new Map();
      for (const event of trackingPlan.events || []) {
        this.schemas.set(`iglu:${trackingPlan.schemaVendor}/${event.name}/jsonschema/${event.version}`, event);
      }
    }

    validate(event) {
      const definition = this.schemas.get(event.schema);
      if (!definition) return { valid: false, reason: "schema_not_found" };
      if (definition.name !== event.event_name) return { valid: false, reason: "event_name_schema_mismatch" };
      const properties = event.properties || {};
      for (const requiredProperty of definition.required || []) {
        if (properties[requiredProperty] == null) {
          return { valid: false, reason: "missing_required_property", property: requiredProperty };
        }
      }
      for (const [name, value] of Object.entries(properties)) {
        const property = definition.properties[name];
        if (!property) return { valid: false, reason: "unknown_property", property: name };
        if (!matchesType(value, property.type)) return { valid: false, reason: "invalid_property_type", property: name };
      }
      return { valid: true };
    }
  }

  class LocalCollector {
    constructor({ broker, registry }) {
      this.broker = broker;
      this.registry = registry;
    }

    async collect(batch) {
      for (const event of batch) {
        this.broker.publish("snowplow_raw_events", event);
        const validation = this.registry.validate(event);
        if (validation.valid) {
          this.broker.publish("snowplow_enriched_events", { ...event, collector_time: event.client_time + 10 });
        } else {
          this.broker.publish("snowplow_bad_events", { ...validation, event });
        }
      }
    }
  }

  class Warehouse {
    constructor() {
      this.tables = new Map();
    }

    insert(table, row) {
      if (!this.tables.has(table)) this.tables.set(table, []);
      this.tables.get(table).push(row);
    }

    table(name) {
      return this.tables.get(name) || [];
    }

    snapshot() {
      return {
        fact_product_exposures: this.table("fact_product_exposures"),
        fact_product_clicks: this.table("fact_product_clicks"),
        fact_page_stays: this.table("fact_page_stays"),
        fact_cart_adds: this.table("fact_cart_adds"),
        snowplow_bad_events: this.table("snowplow_bad_events")
      };
    }
  }

  function createAnalytics(options) {
    const store = options.store || new MemoryEventStore();
    const identity = { anonymous_id: options.anonymousId || "anon-browser", user_id: null, traits: {} };
    let consent = { analyticsAllowed: true, advertisingAllowed: false, piiAllowed: false };
    const session = { session_id: "session-browser", started_at: options.clock() };

    async function track(event) {
      if (!consent.analyticsAllowed) return { accepted: false, reason: "analytics_consent_disabled" };
      const normalized = {
        event_id: options.idGenerator(),
        event_name: event.name,
        schema: event.schema,
        client_time: options.clock(),
        properties: event.properties || {},
        context: {
          app: {
            app_id: options.app.appId,
            platform: options.app.platform,
            app_version: options.app.appVersion,
            sdk_version: options.app.sdkVersion
          },
          user: {
            anonymous_id: identity.anonymous_id,
            user_id: identity.user_id,
            traits: consent.piiAllowed ? identity.traits : {}
          },
          session,
          privacy: consent
        }
      };
      await store.push(normalized);
      return { accepted: true, eventId: normalized.event_id };
    }

    function identify(userId, traits) {
      identity.user_id = userId;
      identity.traits = traits || {};
    }

    async function flush() {
      const batch = await store.peek(50);
      if (batch.length === 0) return { sent: 0, remaining: 0 };
      await options.transport.send(batch);
      await store.remove(batch.length);
      return { sent: batch.length, remaining: await store.size() };
    }

    return { flush, identify, track };
  }

  function createWarehouseConsumer({ broker, warehouse }) {
    let offset = 0;
    return {
      drain() {
        const events = broker.topic("snowplow_enriched_events").slice(offset);
        offset += events.length;
        for (const event of events) routeToWarehouse(warehouse, event);
      }
    };
  }

  function routeToWarehouse(warehouse, event) {
    const user = event.context.user;
    if (event.event_name === "product_exposed") {
      warehouse.insert("fact_product_exposures", {
        event_id: event.event_id,
        product_id: event.properties.product_id,
        page: event.properties.page,
        position: event.properties.position,
        exposure_id: event.properties.exposure_id,
        visible_ratio: event.properties.visible_ratio,
        duration_ms: event.properties.duration_ms,
        recommend_trace_id: event.properties.recommend_trace_id,
        user_id: user.user_id,
        anonymous_id: user.anonymous_id
      });
    }
    if (event.event_name === "product_clicked") {
      warehouse.insert("fact_product_clicks", {
        event_id: event.event_id,
        product_id: event.properties.product_id,
        page: event.properties.page,
        position: event.properties.position,
        click_id: event.properties.click_id,
        recommend_trace_id: event.properties.recommend_trace_id,
        user_id: user.user_id,
        anonymous_id: user.anonymous_id
      });
    }
    if (event.event_name === "page_stay") {
      warehouse.insert("fact_page_stays", {
        event_id: event.event_id,
        page: event.properties.page,
        duration_ms: event.properties.duration_ms,
        stay_id: event.properties.stay_id,
        exit_reason: event.properties.exit_reason,
        user_id: user.user_id,
        anonymous_id: user.anonymous_id
      });
    }
    if (event.event_name === "add_to_cart") {
      warehouse.insert("fact_cart_adds", {
        event_id: event.event_id,
        product_id: event.properties.product_id,
        sku_id: event.properties.sku_id,
        quantity: event.properties.quantity,
        price: event.properties.price,
        currency: event.properties.currency,
        user_id: user.user_id,
        anonymous_id: user.anonymous_id
      });
    }
  }

  function matchesType(value, expectedType) {
    if (expectedType === "integer") return Number.isInteger(value);
    if (expectedType === "number") return typeof value === "number" && Number.isFinite(value);
    return typeof value === expectedType;
  }

  global.OpenEventFlowBrowser = {
    InMemoryTopicBroker,
    LocalCollector,
    TrackingPlanRegistry,
    Warehouse,
    createAnalytics,
    createWarehouseConsumer
  };
})(window);

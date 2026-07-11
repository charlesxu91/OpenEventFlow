function createWarehouseLoader({ adapter }) {
  return {
    async load(events) {
      const rowsByTable = mapEventsToWarehouseRows(events);
      const insertedCounts = {};
      for (const [tableName, rows] of Object.entries(rowsByTable)) {
        if (rows.length > 0) {
          await adapter.insert(tableName, rows);
          insertedCounts[tableName] = rows.length;
        }
      }
      return {
        loaded: events.length,
        tables: insertedCounts
      };
    }
  };
}

function createInMemoryWarehouseAdapter() {
  const tables = new Map();
  return {
    async insert(tableName, rows) {
      if (!tables.has(tableName)) {
        tables.set(tableName, []);
      }
      tables.get(tableName).push(...rows);
    },
    table(tableName) {
      return tables.get(tableName) || [];
    }
  };
}

function createClickHouseAdapter({ client, database = "openeventflow" }) {
  return {
    async insert(tableName, rows) {
      await client.insert({
        table: `${database}.${tableName}`,
        format: "JSONEachRow",
        values: rows
      });
    }
  };
}

function createClickHouseHttpClient({
  endpoint = "http://127.0.0.1:8123",
  username = "default",
  password = "",
  fetch: fetchImpl = globalThis.fetch
}) {
  if (typeof fetchImpl !== "function") {
    throw new Error("fetch is required for createClickHouseHttpClient");
  }

  return {
    async insert({ table, format = "JSONEachRow", values }) {
      const query = `INSERT INTO ${table} FORMAT ${format}`;
      const url = `${endpoint.replace(/\/$/, "")}/?query=${encodeURIComponent(query)}`;
      const response = await fetchImpl(url, {
        method: "POST",
        headers: {
          "content-type": "application/x-ndjson",
          "x-clickhouse-user": username,
          "x-clickhouse-key": password
        },
        body: values.map((row) => JSON.stringify(row)).join("\n") + "\n"
      });
      if (!response.ok) {
        const body = typeof response.text === "function" ? await response.text() : "";
        throw new Error(`ClickHouse insert failed: ${response.status} ${body}`.trim());
      }
      return { inserted: values.length };
    }
  };
}

function createKafkaWarehouseConsumer({
  kafka,
  loader,
  topic,
  groupId = "openeventflow-warehouse",
  fromBeginning = false,
  parseMessage = parseJsonMessage
}) {
  return {
    async start() {
      const consumer = kafka.consumer({ groupId });
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning });
      await consumer.run({
        eachBatch: async ({ batch }) => {
          const events = batch.messages.map(parseMessage).filter(Boolean);
          if (events.length > 0) {
            await loader.load(events);
          }
        }
      });
      return consumer;
    }
  };
}

function parseJsonMessage(message) {
  if (!message || !message.value) {
    return null;
  }
  return JSON.parse(message.value.toString("utf8"));
}

function mapEventsToWarehouseRows(events) {
  const rowsByTable = {
    ods_snowplow_enriched_events: [],
    dwd_app_behavior_events: [],
    fact_product_exposures: [],
    fact_product_clicks: [],
    fact_page_stays: [],
    fact_cart_adds: [],
    fact_recommendation_events: [],
    fact_order_events: [],
    fact_video_exposures: [],
    fact_video_plays: [],
    fact_video_watches: [],
    fact_video_engagements: [],
    ads_product_behavior_daily: [],
    ads_video_behavior_daily: []
  };
  const productDaily = new Map();
  const videoDaily = new Map();

  for (const event of events) {
    rowsByTable.ods_snowplow_enriched_events.push(odsRow(event));
    rowsByTable.dwd_app_behavior_events.push(dwdRow(event));

    if (isRecommendationEvent(event)) {
      rowsByTable.fact_recommendation_events.push(recommendationEventRow(event));
    }
    if (isOrderEvent(event)) {
      rowsByTable.fact_order_events.push(orderEventRow(event));
    }

    if (event.event_name === "product_exposed" || event.event_name === "product_impressed") {
      const row = exposureRow(event);
      rowsByTable.fact_product_exposures.push(row);
      incrementProductDaily(productDaily, row.event_date, row.product_id, { exposures: 1 });
    }
    if (event.event_name === "product_clicked") {
      const row = clickRow(event);
      rowsByTable.fact_product_clicks.push(row);
      incrementProductDaily(productDaily, row.event_date, row.product_id, { clicks: 1 });
    }
    if (event.event_name === "page_stay") {
      rowsByTable.fact_page_stays.push(stayRow(event));
    }
    if (event.event_name === "add_to_cart") {
      const row = cartRow(event);
      rowsByTable.fact_cart_adds.push(row);
      incrementProductDaily(productDaily, row.event_date, row.product_id, {
        cart_adds: 1,
        cart_quantity: row.quantity,
        cart_gmv: row.gmv
      });
    }
    if (event.event_name === "video_exposed") {
      const row = videoExposureRow(event);
      rowsByTable.fact_video_exposures.push(row);
      incrementVideoDaily(videoDaily, row.event_date, row.video_id, row.author_id, { exposures: 1 });
    }
    if (event.event_name === "video_played") {
      const row = videoPlayRow(event);
      rowsByTable.fact_video_plays.push(row);
      incrementVideoDaily(videoDaily, row.event_date, row.video_id, row.author_id, { plays: 1 });
    }
    if (event.event_name === "video_watch") {
      const row = videoWatchRow(event);
      rowsByTable.fact_video_watches.push(row);
      incrementVideoDaily(videoDaily, row.event_date, row.video_id, row.author_id, {
        watch_ms: row.duration_ms,
        completed_plays: row.completed ? 1 : 0
      });
    }
    if (event.event_name === "video_engaged") {
      const row = videoEngagementRow(event);
      rowsByTable.fact_video_engagements.push(row);
      incrementVideoDaily(videoDaily, row.event_date, row.video_id, row.author_id, videoEngagementIncrement(row.action));
    }
  }

  rowsByTable.ads_product_behavior_daily.push(...productDaily.values());
  rowsByTable.ads_video_behavior_daily.push(...videoDaily.values());
  return rowsByTable;
}

function isRecommendationEvent(event) {
  return [
    "recommendation_delivered",
    "product_impressed",
    "product_exposed",
    "product_clicked",
    "favorite_added",
    "add_to_cart"
  ].includes(event.event_name);
}

function isOrderEvent(event) {
  return ["order_created", "order_paid", "order_cancelled", "order_refunded"].includes(event.event_name);
}

function recommendationEventRow(event) {
  const properties = event.properties || {};
  return {
    event_id: event.event_id,
    event_name: event.event_name,
    event_date: eventDate(event.client_time),
    event_time: event.client_time,
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    request_id: properties.request_id || properties.recommend_trace_id || null,
    impression_id: properties.impression_id || properties.exposure_id || null,
    delivery_id: properties.delivery_id || null,
    product_id: properties.product_id,
    sku_id: properties.sku_id || null,
    surface: properties.surface || properties.page || null,
    position: properties.position === undefined ? null : properties.position,
    candidate_source: properties.candidate_source || null,
    model_version: properties.model_version || null,
    feature_set_version: properties.feature_set_version || null,
    experiment_id: properties.experiment_id || null,
    experiment_treatment: properties.experiment_treatment || null,
    recommendation_generation: properties.recommendation_generation || null
  };
}

function orderEventRow(event) {
  const properties = event.properties || {};
  return {
    event_id: event.event_id,
    event_name: event.event_name,
    event_date: eventDate(event.client_time),
    event_time: event.client_time,
    user_id: userId(event),
    order_id: properties.order_id,
    order_line_id: properties.order_line_id,
    product_id: properties.product_id,
    sku_id: properties.sku_id,
    quantity: properties.quantity,
    amount: properties.paid_amount === undefined
      ? (properties.refund_amount === undefined ? properties.unit_price * properties.quantity : properties.refund_amount)
      : properties.paid_amount,
    currency: properties.currency || null,
    request_id: properties.request_id || null,
    impression_id: properties.impression_id || null,
    delivery_id: properties.delivery_id || null
  };
}

function odsRow(event) {
  return {
    event_id: event.event_id,
    event_name: event.event_name,
    schema: event.schema,
    collector_time: event.collector_time || null,
    enriched_at: event.enriched_at || null,
    event_json: JSON.stringify(event)
  };
}

function dwdRow(event) {
  return {
    event_id: event.event_id,
    event_name: event.event_name,
    schema: event.schema,
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    app_id: appId(event),
    platform: platform(event),
    event_date: eventDate(event.client_time),
    event_time: event.client_time,
    collector_time: event.collector_time || null,
    enriched_at: event.enriched_at || null,
    properties: JSON.stringify(event.properties || {})
  };
}

function exposureRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    product_id: event.properties.product_id,
    page: event.properties.page,
    position: event.properties.position,
    exposure_id: event.properties.exposure_id,
    visible_ratio: event.properties.visible_ratio,
    duration_ms: event.properties.duration_ms,
    recommend_trace_id: event.properties.recommend_trace_id || null,
    event_time: event.client_time
  };
}

function clickRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    product_id: event.properties.product_id,
    page: event.properties.page,
    position: event.properties.position,
    click_id: event.properties.click_id,
    recommend_trace_id: event.properties.recommend_trace_id || null,
    event_time: event.client_time
  };
}

function stayRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    page: event.properties.page,
    duration_ms: event.properties.duration_ms,
    stay_id: event.properties.stay_id,
    exit_reason: event.properties.exit_reason || null,
    event_time: event.client_time
  };
}

function cartRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    product_id: event.properties.product_id,
    sku_id: event.properties.sku_id,
    quantity: event.properties.quantity,
    price: event.properties.price,
    currency: event.properties.currency || null,
    gmv: event.properties.quantity * event.properties.price,
    event_time: event.client_time
  };
}

function videoExposureRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    video_id: event.properties.video_id,
    author_id: event.properties.author_id || null,
    page: event.properties.page,
    position: event.properties.position,
    exposure_id: event.properties.exposure_id,
    visible_ratio: event.properties.visible_ratio,
    duration_ms: event.properties.duration_ms,
    recommend_trace_id: event.properties.recommend_trace_id || null,
    event_time: event.client_time
  };
}

function videoPlayRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    video_id: event.properties.video_id,
    author_id: event.properties.author_id || null,
    page: event.properties.page,
    position: event.properties.position,
    play_id: event.properties.play_id,
    autoplay: event.properties.autoplay === undefined ? null : Boolean(event.properties.autoplay),
    network_type: event.properties.network_type || null,
    recommend_trace_id: event.properties.recommend_trace_id || null,
    event_time: event.client_time
  };
}

function videoWatchRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    video_id: event.properties.video_id,
    author_id: event.properties.author_id || null,
    play_id: event.properties.play_id,
    watch_id: event.properties.watch_id,
    duration_ms: event.properties.duration_ms,
    play_duration_ms: event.properties.play_duration_ms || null,
    completion_rate: event.properties.completion_rate || null,
    completed: Boolean(event.properties.completed),
    exit_reason: event.properties.exit_reason || null,
    event_time: event.client_time
  };
}

function videoEngagementRow(event) {
  return {
    event_id: event.event_id,
    event_date: eventDate(event.client_time),
    user_id: userId(event),
    anonymous_id: anonymousId(event),
    video_id: event.properties.video_id,
    author_id: event.properties.author_id || null,
    play_id: event.properties.play_id || null,
    engagement_id: event.properties.engagement_id,
    action: event.properties.action,
    event_time: event.client_time
  };
}

function incrementProductDaily(productDaily, eventDateValue, productId, values) {
  const key = `${eventDateValue}:${productId}`;
  if (!productDaily.has(key)) {
    productDaily.set(key, {
      event_date: eventDateValue,
      product_id: productId,
      exposures: 0,
      clicks: 0,
      cart_adds: 0,
      cart_quantity: 0,
      cart_gmv: 0
    });
  }
  const row = productDaily.get(key);
  row.exposures += values.exposures || 0;
  row.clicks += values.clicks || 0;
  row.cart_adds += values.cart_adds || 0;
  row.cart_quantity += values.cart_quantity || 0;
  row.cart_gmv += values.cart_gmv || 0;
}

function incrementVideoDaily(videoDaily, eventDateValue, videoId, authorId, values) {
  const key = `${eventDateValue}:${videoId}`;
  if (!videoDaily.has(key)) {
    videoDaily.set(key, {
      event_date: eventDateValue,
      video_id: videoId,
      author_id: authorId || null,
      exposures: 0,
      plays: 0,
      watch_ms: 0,
      completed_plays: 0,
      likes: 0,
      comments: 0,
      shares: 0,
      follows: 0
    });
  }
  const row = videoDaily.get(key);
  if (!row.author_id && authorId) {
    row.author_id = authorId;
  }
  row.exposures += values.exposures || 0;
  row.plays += values.plays || 0;
  row.watch_ms += values.watch_ms || 0;
  row.completed_plays += values.completed_plays || 0;
  row.likes += values.likes || 0;
  row.comments += values.comments || 0;
  row.shares += values.shares || 0;
  row.follows += values.follows || 0;
}

function videoEngagementIncrement(action) {
  if (action === "like") {
    return { likes: 1 };
  }
  if (action === "comment") {
    return { comments: 1 };
  }
  if (action === "share") {
    return { shares: 1 };
  }
  if (action === "follow") {
    return { follows: 1 };
  }
  return {};
}

function userId(event) {
  return event.context && event.context.user ? event.context.user.user_id || null : null;
}

function anonymousId(event) {
  return event.context && event.context.user ? event.context.user.anonymous_id || null : null;
}

function appId(event) {
  return event.context && event.context.app ? event.context.app.app_id || event.context.app.appId || null : null;
}

function platform(event) {
  return event.context && event.context.app ? event.context.app.platform || null : null;
}

function eventDate(timestampMillis) {
  return new Date(timestampMillis).toISOString().slice(0, 10);
}

module.exports = {
  createClickHouseAdapter,
  createClickHouseHttpClient,
  createInMemoryWarehouseAdapter,
  createKafkaWarehouseConsumer,
  createWarehouseLoader,
  mapEventsToWarehouseRows
};

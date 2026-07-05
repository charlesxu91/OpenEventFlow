const fs = require("node:fs");
const path = require("node:path");
const { createAnalytics, MemoryEventStore } = require("../../packages/core/src/index");
const {
  InMemoryTopicBroker,
  LocalCollector,
  TrackingPlanRegistry,
  Warehouse,
  createEcommerceWarehouseConsumer
} = require("./pipeline");

async function runUiDrivenEcommerceJourney() {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.join(__dirname, "../../examples/tracking-plan.json"), "utf8")
  );
  const broker = new InMemoryTopicBroker();
  const warehouse = new Warehouse();
  const collector = new LocalCollector({
    broker,
    registry: new TrackingPlanRegistry(trackingPlan)
  });
  const consumer = createEcommerceWarehouseConsumer({ broker, warehouse, topic: "snowplow_enriched_events" });
  const ids = ["evt-exp-ui", "evt-click-ui", "evt-stay-ui", "evt-cart-ui"];
  const analytics = createAnalytics({
    app: { appId: "shop-ui", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: { send: async (batch) => collector.collect(batch) },
    anonymousId: "anon-ui",
    idGenerator: () => ids.shift() || "evt-extra",
    clock: deterministicClock([900, 1000, 1100, 1200, 1300])
  });
  analytics.identify("user-ui-42", { segment: "vip" });

  await onProductVisible(analytics);
  await flushAndDrain(analytics, consumer);
  await onProductCardClicked(analytics);
  await flushAndDrain(analytics, consumer);
  await onAddToCartClicked(analytics);
  await flushAndDrain(analytics, consumer);

  const snapshot = snapshotWarehouse(warehouse);
  return {
    snapshot,
    counts: {
      exposures: snapshot.fact_product_exposures.length,
      clicks: snapshot.fact_product_clicks.length,
      stays: snapshot.fact_page_stays.length,
      carts: snapshot.fact_cart_adds.length,
      badEvents: broker.topic("snowplow_bad_events").length
    }
  };
}

async function onProductVisible(analytics) {
  await analytics.track({
    name: "product_exposed",
    schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
    properties: {
      product_id: "sku-ui-100",
      page: "home_feed",
      position: 1,
      exposure_id: "exp-ui-100",
      visible_ratio: 0.91,
      duration_ms: 1500,
      recommend_trace_id: "rec-ui-abc"
    }
  });
}

async function onProductCardClicked(analytics) {
  await analytics.track({
    name: "product_clicked",
    schema: "iglu:io.openeventflow/product_clicked/jsonschema/1-0-0",
    properties: {
      product_id: "sku-ui-100",
      page: "home_feed",
      position: 1,
      click_id: "clk-ui-100",
      recommend_trace_id: "rec-ui-abc"
    }
  });
}

async function onAddToCartClicked(analytics) {
  await analytics.track({
    name: "page_stay",
    schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
    properties: {
      page: "product_detail",
      duration_ms: 8200,
      stay_id: "stay-ui-100",
      exit_reason: "add_to_cart"
    }
  });
  await analytics.track({
    name: "add_to_cart",
    schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
    properties: {
      product_id: "sku-ui-100",
      sku_id: "sku-ui-100-blue",
      quantity: 1,
      price: 88.5,
      currency: "CNY"
    }
  });
}

async function flushAndDrain(analytics, consumer) {
  await analytics.flush();
  await consumer.drain();
}

function snapshotWarehouse(warehouse) {
  return {
    fact_product_exposures: warehouse.table("fact_product_exposures"),
    fact_product_clicks: warehouse.table("fact_product_clicks"),
    fact_page_stays: warehouse.table("fact_page_stays"),
    fact_cart_adds: warehouse.table("fact_cart_adds")
  };
}

function deterministicClock(values) {
  let index = 0;
  return () => values[index++] || values[values.length - 1];
}

module.exports = {
  runUiDrivenEcommerceJourney,
  runUiDrivenShortVideoJourney
};

async function runUiDrivenShortVideoJourney() {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.join(__dirname, "../../examples/tracking-plan.json"), "utf8")
  );
  const broker = new InMemoryTopicBroker();
  const warehouse = new Warehouse();
  const collector = new LocalCollector({
    broker,
    registry: new TrackingPlanRegistry(trackingPlan)
  });
  const consumer = createEcommerceWarehouseConsumer({ broker, warehouse, topic: "snowplow_enriched_events" });
  const ids = ["evt-vexp-ui", "evt-vplay-ui", "evt-vwatch-ui", "evt-vlike-ui", "evt-vshare-ui"];
  const analytics = createAnalytics({
    app: { appId: "video-ui", platform: "react-native", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: { send: async (batch) => collector.collect(batch) },
    anonymousId: "anon-video-ui",
    idGenerator: () => ids.shift() || "evt-video-extra",
    clock: deterministicClock([900, 1000, 1100, 1200, 1300, 1400])
  });
  analytics.identify("video-ui-user-42", { segment: "feed_power_user" });

  await onVideoVisible(analytics);
  await flushAndDrain(analytics, consumer);
  await onVideoAutoplayStarted(analytics);
  await flushAndDrain(analytics, consumer);
  await onVideoSwipedAway(analytics);
  await flushAndDrain(analytics, consumer);
  await onVideoLiked(analytics);
  await onVideoShared(analytics);
  await flushAndDrain(analytics, consumer);

  const snapshot = {
    fact_video_exposures: warehouse.table("fact_video_exposures"),
    fact_video_plays: warehouse.table("fact_video_plays"),
    fact_video_watches: warehouse.table("fact_video_watches"),
    fact_video_engagements: warehouse.table("fact_video_engagements")
  };
  return {
    snapshot,
    counts: {
      exposures: snapshot.fact_video_exposures.length,
      plays: snapshot.fact_video_plays.length,
      watches: snapshot.fact_video_watches.length,
      engagements: snapshot.fact_video_engagements.length,
      badEvents: broker.topic("snowplow_bad_events").length
    }
  };
}

async function onVideoVisible(analytics) {
  await analytics.track({
    name: "video_exposed",
    schema: "iglu:io.openeventflow/video_exposed/jsonschema/1-0-0",
    properties: {
      video_id: "video-ui-100",
      author_id: "author-ui-7",
      page: "video_feed",
      position: 1,
      exposure_id: "vexp-ui-100",
      visible_ratio: 0.98,
      duration_ms: 1800,
      recommend_trace_id: "vrec-ui-abc"
    }
  });
}

async function onVideoAutoplayStarted(analytics) {
  await analytics.track({
    name: "video_played",
    schema: "iglu:io.openeventflow/video_played/jsonschema/1-0-0",
    properties: {
      video_id: "video-ui-100",
      author_id: "author-ui-7",
      page: "video_feed",
      position: 1,
      play_id: "play-ui-100",
      autoplay: true,
      network_type: "5g",
      recommend_trace_id: "vrec-ui-abc"
    }
  });
}

async function onVideoSwipedAway(analytics) {
  await analytics.track({
    name: "video_watch",
    schema: "iglu:io.openeventflow/video_watch/jsonschema/1-0-0",
    properties: {
      video_id: "video-ui-100",
      author_id: "author-ui-7",
      play_id: "play-ui-100",
      watch_id: "watch-ui-100",
      duration_ms: 12500,
      play_duration_ms: 15000,
      completion_rate: 0.83,
      completed: false,
      exit_reason: "swipe_next"
    }
  });
}

async function onVideoLiked(analytics) {
  await analytics.track({
    name: "video_engaged",
    schema: "iglu:io.openeventflow/video_engaged/jsonschema/1-0-0",
    properties: {
      video_id: "video-ui-100",
      author_id: "author-ui-7",
      play_id: "play-ui-100",
      engagement_id: "vlike-ui-100",
      action: "like"
    }
  });
}

async function onVideoShared(analytics) {
  await analytics.track({
    name: "video_engaged",
    schema: "iglu:io.openeventflow/video_engaged/jsonschema/1-0-0",
    properties: {
      video_id: "video-ui-100",
      author_id: "author-ui-7",
      play_id: "play-ui-100",
      engagement_id: "vshare-ui-100",
      action: "share"
    }
  });
}

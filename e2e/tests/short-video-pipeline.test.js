const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const { createAnalytics, MemoryEventStore } = require("../../packages/core/src/index");
const {
  InMemoryTopicBroker,
  LocalCollector,
  TrackingPlanRegistry,
  Warehouse,
  createEcommerceWarehouseConsumer
} = require("../src/pipeline");
const { runShortVideoFeedJourney } = require("../src/sample-short-video-app");
const { runUiDrivenShortVideoJourney } = require("../src/ui-model");

test("short-video feed SDK events flow through collector, broker, and warehouse facts", async () => {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.join(__dirname, "../../examples/tracking-plan.json"), "utf8")
  );
  const broker = new InMemoryTopicBroker();
  const collector = new LocalCollector({
    broker,
    registry: new TrackingPlanRegistry(trackingPlan)
  });
  const warehouse = new Warehouse();
  const consumer = createEcommerceWarehouseConsumer({ broker, warehouse, topic: "snowplow_enriched_events" });
  const analytics = createAnalytics({
    app: { appId: "video-demo", platform: "react-native", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    store: new MemoryEventStore(),
    transport: { send: async (batch) => collector.collect(batch) },
    anonymousId: "anon-video-1",
    idGenerator: deterministicIds(["evt-vexp", "evt-vplay", "evt-vwatch", "evt-vlike", "session-2"]),
    clock: deterministicClock([900, 1000, 1100, 1200, 1300])
  });

  await runShortVideoFeedJourney(analytics);
  const flushResult = await analytics.flush();
  await consumer.drain();

  assert.deepEqual(flushResult, { sent: 4, remaining: 0 });
  assert.equal(broker.topic("snowplow_bad_events").length, 0);
  assert.equal(warehouse.table("fact_video_exposures").length, 1);
  assert.equal(warehouse.table("fact_video_plays").length, 1);
  assert.equal(warehouse.table("fact_video_watches").length, 1);
  assert.equal(warehouse.table("fact_video_engagements").length, 1);
  assert.deepEqual(warehouse.table("fact_video_exposures")[0], {
    event_id: "evt-vexp",
    event_date: "1970-01-01",
    user_id: "video-user-42",
    anonymous_id: "anon-video-1",
    video_id: "video-100",
    author_id: "author-7",
    page: "video_feed",
    position: 3,
    exposure_id: "vexp-100",
    visible_ratio: 0.96,
    duration_ms: 1600,
    recommend_trace_id: "vrec-abc",
    event_time: 1000
  });
  assert.equal(warehouse.table("fact_video_plays")[0].play_id, "play-100");
  assert.equal(warehouse.table("fact_video_watches")[0].completion_rate, 0.82);
  assert.equal(warehouse.table("fact_video_engagements")[0].action, "like");
  assert.deepEqual(warehouse.table("ads_video_behavior_daily")[0], {
    event_date: "1970-01-01",
    video_id: "video-100",
    author_id: "author-7",
    exposures: 1,
    plays: 1,
    watch_ms: 8200,
    completed_plays: 0,
    likes: 1,
    comments: 0,
    shares: 0,
    follows: 0
  });
});

test("short-video UI model triggers exposure, play, watch, and engagement consistently", async () => {
  const result = await runUiDrivenShortVideoJourney();

  assert.deepEqual(result.counts, {
    exposures: 1,
    plays: 1,
    watches: 1,
    engagements: 2,
    badEvents: 0
  });
  assert.equal(result.snapshot.fact_video_exposures[0].video_id, "video-ui-100");
  assert.equal(result.snapshot.fact_video_plays[0].play_id, "play-ui-100");
  assert.equal(result.snapshot.fact_video_watches[0].duration_ms, 12500);
  assert.equal(result.snapshot.fact_video_engagements[0].action, "like");
  assert.equal(result.snapshot.fact_video_engagements[1].action, "share");
});

function deterministicIds(values) {
  let index = 0;
  return () => values[index++] || `evt-${index}`;
}

function deterministicClock(values) {
  let index = 0;
  return () => values[index++] || values[values.length - 1];
}

#!/usr/bin/env node

const fs = require("node:fs");
const { execFileSync } = require("node:child_process");
const path = require("node:path");
const {
  createClickHouseAdapter,
  createClickHouseHttpClient,
  createWarehouseLoader
} = require("../packages/warehouse/src/index");

const root = path.join(__dirname, "..");
const composeFile = path.join(root, "deploy/docker/docker-compose.yml");
const runId = `video_${Date.now()}`;
const topic = `openeventflow_${runId}`;
const videoId = `video_${runId}`;
const authorId = `author_${runId}`;
const events = createVideoEvents(runId, videoId, authorId);

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});

async function main() {
  dockerCompose(["up", "-d"]);
  await waitForClickHouse();
  await applyClickHouseSchema();
  dockerCompose(["exec", "-T", "redpanda", "rpk", "topic", "create", topic, "--brokers", "redpanda:9092"]);
  dockerCompose(
    ["exec", "-T", "redpanda", "rpk", "topic", "produce", topic, "--brokers", "redpanda:9092"],
    events.map((event) => JSON.stringify(event)).join("\n") + "\n"
  );

  const consumed = consumeEvents(topic, events.length);
  const loader = createWarehouseLoader({
    adapter: createClickHouseAdapter({
      database: "openeventflow",
      client: createClickHouseHttpClient({
        endpoint: "http://127.0.0.1:8123",
        username: "openeventflow",
        password: "openeventflow"
      })
    })
  });
  const loadResult = await loader.load(consumed);
  const verification = await verifyClickHouse(videoId, events.map((event) => event.event_id));

  console.log(JSON.stringify({ runId, topic, loadResult, verification }, null, 2));
}

function dockerCompose(args, input) {
  return execFileSync("docker", ["compose", "-f", composeFile, ...args], {
    cwd: root,
    encoding: "utf8",
    input,
    stdio: input === undefined ? ["ignore", "pipe", "inherit"] : ["pipe", "pipe", "inherit"]
  });
}

async function waitForClickHouse() {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch("http://127.0.0.1:8123/ping", { headers: clickHouseHeaders() });
      if (response.ok && (await response.text()).trim() === "Ok.") {
        return;
      }
    } catch (_error) {
      // Retry while Docker starts ClickHouse.
    }
    await sleep(1000);
  }
  throw new Error("ClickHouse did not become ready within 60s");
}

async function applyClickHouseSchema() {
  const ddl = fs.readFileSync(path.join(root, "deploy/docker/init-clickhouse.sql"), "utf8");
  for (const statement of splitSqlStatements(ddl)) {
    const response = await fetch(`http://127.0.0.1:8123/?query=${encodeURIComponent(statement)}`, {
      method: "POST",
      headers: clickHouseHeaders()
    });
    if (!response.ok) {
      throw new Error(`ClickHouse DDL failed: ${response.status} ${await response.text()}`);
    }
  }
}

function consumeEvents(topicName, expectedCount) {
  const output = dockerCompose([
    "exec",
    "-T",
    "redpanda",
    "rpk",
    "topic",
    "consume",
    topicName,
    "--brokers",
    "redpanda:9092",
    "--num",
    String(expectedCount)
  ]);
  const messages = parseJsonObjects(output).map((message) => JSON.parse(message.value));
  if (messages.length !== expectedCount) {
    throw new Error(`expected ${expectedCount} consumed messages, got ${messages.length}`);
  }
  return messages;
}

async function verifyClickHouse(verifiedVideoId, eventIds) {
  const quotedIds = eventIds.map((id) => `'${id}'`).join(",");
  const query = `
    SELECT
      (SELECT count() FROM openeventflow.fact_video_exposures WHERE event_id IN (${quotedIds})) AS exposures,
      (SELECT count() FROM openeventflow.fact_video_plays WHERE event_id IN (${quotedIds})) AS plays,
      (SELECT count() FROM openeventflow.fact_video_watches WHERE event_id IN (${quotedIds})) AS watches,
      (SELECT count() FROM openeventflow.fact_video_engagements WHERE event_id IN (${quotedIds})) AS engagements,
      (SELECT sum(exposures) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_exposures,
      (SELECT sum(plays) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_plays,
      (SELECT sum(watch_ms) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_watch_ms,
      (SELECT sum(completed_plays) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_completed,
      (SELECT sum(likes) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_likes,
      (SELECT sum(shares) FROM openeventflow.ads_video_behavior_daily WHERE video_id = '${verifiedVideoId}') AS ads_shares
    FORMAT JSONEachRow
  `;
  const response = await fetch(`http://127.0.0.1:8123/?query=${encodeURIComponent(query)}`, {
    headers: clickHouseHeaders()
  });
  if (!response.ok) {
    throw new Error(`ClickHouse verification failed: ${response.status} ${await response.text()}`);
  }
  const row = JSON.parse((await response.text()).trim());
  const expected = {
    exposures: 1,
    plays: 1,
    watches: 1,
    engagements: 2,
    ads_exposures: 1,
    ads_plays: 1,
    ads_watch_ms: 12500,
    ads_completed: 1,
    ads_likes: 1,
    ads_shares: 1
  };
  for (const [key, value] of Object.entries(expected)) {
    if (Number(row[key]) !== value) {
      throw new Error(`ClickHouse mismatch for ${key}: expected ${value}, got ${row[key]}`);
    }
  }
  return row;
}

function createVideoEvents(id, currentVideoId, currentAuthorId) {
  const base = 1783232000000;
  const context = {
    user: { user_id: "video-user-42", anonymous_id: `anon_${id}` },
    app: { app_id: "video-demo", platform: "docker-video-smoke" }
  };
  return [
    {
      event_id: `${id}_exposure`,
      event_name: "video_exposed",
      schema: "iglu:io.openeventflow/video_exposed/jsonschema/1-0-0",
      client_time: base,
      collector_time: base + 10,
      enriched_at: base + 20,
      properties: {
        video_id: currentVideoId,
        author_id: currentAuthorId,
        page: "video_feed",
        position: 1,
        exposure_id: `${id}_exposure_id`,
        visible_ratio: 0.99,
        duration_ms: 1800,
        recommend_trace_id: `${id}_rec`
      },
      context
    },
    {
      event_id: `${id}_play`,
      event_name: "video_played",
      schema: "iglu:io.openeventflow/video_played/jsonschema/1-0-0",
      client_time: base + 1000,
      collector_time: base + 1010,
      enriched_at: base + 1020,
      properties: {
        video_id: currentVideoId,
        author_id: currentAuthorId,
        page: "video_feed",
        position: 1,
        play_id: `${id}_play_id`,
        autoplay: true,
        network_type: "wifi",
        recommend_trace_id: `${id}_rec`
      },
      context
    },
    {
      event_id: `${id}_watch`,
      event_name: "video_watch",
      schema: "iglu:io.openeventflow/video_watch/jsonschema/1-0-0",
      client_time: base + 2000,
      collector_time: base + 2010,
      enriched_at: base + 2020,
      properties: {
        video_id: currentVideoId,
        author_id: currentAuthorId,
        play_id: `${id}_play_id`,
        watch_id: `${id}_watch_id`,
        duration_ms: 12500,
        play_duration_ms: 12500,
        completion_rate: 1,
        completed: true,
        exit_reason: "complete"
      },
      context
    },
    videoEngagementEvent(id, currentVideoId, currentAuthorId, base + 3000, "like"),
    videoEngagementEvent(id, currentVideoId, currentAuthorId, base + 4000, "share")
  ];
}

function videoEngagementEvent(id, currentVideoId, currentAuthorId, clientTime, action) {
  return {
    event_id: `${id}_${action}`,
    event_name: "video_engaged",
    schema: "iglu:io.openeventflow/video_engaged/jsonschema/1-0-0",
    client_time: clientTime,
    collector_time: clientTime + 10,
    enriched_at: clientTime + 20,
    properties: {
      video_id: currentVideoId,
      author_id: currentAuthorId,
      play_id: `${id}_play_id`,
      engagement_id: `${id}_${action}_id`,
      action
    },
    context: {
      user: { user_id: "video-user-42", anonymous_id: `anon_${id}` },
      app: { app_id: "video-demo", platform: "docker-video-smoke" }
    }
  };
}

function splitSqlStatements(sql) {
  return sql
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function parseJsonObjects(output) {
  const objects = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < output.length; index += 1) {
    const char = output[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }
    if (char === "\"") {
      inString = true;
    } else if (char === "{") {
      if (depth === 0) {
        start = index;
      }
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        objects.push(JSON.parse(output.slice(start, index + 1)));
        start = -1;
      }
    }
  }
  return objects;
}

function clickHouseHeaders() {
  return {
    "x-clickhouse-user": "openeventflow",
    "x-clickhouse-key": "openeventflow"
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

#!/usr/bin/env node

const { execFileSync } = require("node:child_process");
const path = require("node:path");
const {
  createClickHouseAdapter,
  createClickHouseHttpClient,
  createWarehouseLoader
} = require("../packages/warehouse/src/index");

const root = path.join(__dirname, "..");
const composeFile = path.join(root, "deploy/docker/docker-compose.yml");
const runId = `smoke_${Date.now()}`;
const topic = `openeventflow_${runId}`;
const productId = `sku_${runId}`;
const events = createEvents(runId, productId);

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});

async function main() {
  dockerCompose(["up", "-d"]);
  await waitForClickHouse();
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
  const verification = await verifyClickHouse(productId, events.map((event) => event.event_id));

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
      const response = await fetch("http://127.0.0.1:8123/ping", {
        headers: {
          "x-clickhouse-user": "openeventflow",
          "x-clickhouse-key": "openeventflow"
        }
      });
      if (response.ok && (await response.text()).trim() === "Ok.") {
        return;
      }
    } catch (_error) {
      // Retry until Docker finishes starting ClickHouse.
    }
    await sleep(1000);
  }
  throw new Error("ClickHouse did not become ready within 60s");
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

async function verifyClickHouse(verifiedProductId, eventIds) {
  const quotedIds = eventIds.map((id) => `'${id}'`).join(",");
  const query = `
    SELECT
      (SELECT count() FROM openeventflow.fact_product_exposures WHERE event_id IN (${quotedIds})) AS exposures,
      (SELECT count() FROM openeventflow.fact_product_clicks WHERE event_id IN (${quotedIds})) AS clicks,
      (SELECT count() FROM openeventflow.fact_page_stays WHERE event_id IN (${quotedIds})) AS stays,
      (SELECT count() FROM openeventflow.fact_cart_adds WHERE event_id IN (${quotedIds})) AS carts,
      (SELECT sum(exposures) FROM openeventflow.ads_product_behavior_daily WHERE product_id = '${verifiedProductId}') AS ads_exposures,
      (SELECT sum(clicks) FROM openeventflow.ads_product_behavior_daily WHERE product_id = '${verifiedProductId}') AS ads_clicks,
      (SELECT sum(cart_adds) FROM openeventflow.ads_product_behavior_daily WHERE product_id = '${verifiedProductId}') AS ads_carts,
      (SELECT sum(cart_quantity) FROM openeventflow.ads_product_behavior_daily WHERE product_id = '${verifiedProductId}') AS ads_quantity,
      (SELECT sum(cart_gmv) FROM openeventflow.ads_product_behavior_daily WHERE product_id = '${verifiedProductId}') AS ads_gmv
    FORMAT JSONEachRow
  `;
  const response = await fetch(`http://127.0.0.1:8123/?query=${encodeURIComponent(query)}`, {
    headers: {
      "x-clickhouse-user": "openeventflow",
      "x-clickhouse-key": "openeventflow"
    }
  });
  if (!response.ok) {
    throw new Error(`ClickHouse verification failed: ${response.status} ${await response.text()}`);
  }
  const row = JSON.parse((await response.text()).trim());
  const expected = {
    exposures: 1,
    clicks: 1,
    stays: 1,
    carts: 1,
    ads_exposures: 1,
    ads_clicks: 1,
    ads_carts: 1,
    ads_quantity: 2,
    ads_gmv: 398
  };
  for (const [key, value] of Object.entries(expected)) {
    if (Number(row[key]) !== value) {
      throw new Error(`ClickHouse mismatch for ${key}: expected ${value}, got ${row[key]}`);
    }
  }
  return row;
}

function createEvents(id, sku) {
  const base = 1783231000000;
  const context = {
    user: { user_id: "user-42", anonymous_id: `anon_${id}` },
    app: { app_id: "shop-demo", platform: "docker-smoke" }
  };
  return [
    {
      event_id: `${id}_exp`,
      event_name: "product_exposed",
      schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
      client_time: base,
      collector_time: base + 10,
      enriched_at: base + 20,
      properties: {
        product_id: sku,
        page: "home_feed",
        position: 1,
        exposure_id: `${id}_exposure`,
        visible_ratio: 0.9,
        duration_ms: 1300,
        recommend_trace_id: `${id}_rec`
      },
      context
    },
    {
      event_id: `${id}_click`,
      event_name: "product_clicked",
      schema: "iglu:io.openeventflow/product_clicked/jsonschema/1-0-0",
      client_time: base + 1000,
      collector_time: base + 1010,
      enriched_at: base + 1020,
      properties: {
        product_id: sku,
        page: "home_feed",
        position: 1,
        click_id: `${id}_click_id`,
        recommend_trace_id: `${id}_rec`
      },
      context
    },
    {
      event_id: `${id}_stay`,
      event_name: "page_stay",
      schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
      client_time: base + 2000,
      collector_time: base + 2010,
      enriched_at: base + 2020,
      properties: {
        page: "product_detail",
        duration_ms: 8200,
        stay_id: `${id}_stay_id`,
        exit_reason: "add_to_cart"
      },
      context
    },
    {
      event_id: `${id}_cart`,
      event_name: "add_to_cart",
      schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
      client_time: base + 3000,
      collector_time: base + 3010,
      enriched_at: base + 3020,
      properties: {
        product_id: sku,
        sku_id: `${sku}_blue`,
        quantity: 2,
        price: 199,
        currency: "CNY"
      },
      context
    }
  ];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

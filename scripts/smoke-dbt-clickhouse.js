#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const requiredModels = [
  "warehouse/dbt/models/dwd/dwd_app_behavior_events.sql",
  "warehouse/dbt/models/fct/fact_product_exposures.sql",
  "warehouse/dbt/models/fct/fact_product_clicks.sql",
  "warehouse/dbt/models/fct/fact_page_stays.sql",
  "warehouse/dbt/models/fct/fact_cart_adds.sql",
  "warehouse/dbt/models/ads/ads_product_behavior_daily.sql"
];

async function main() {
  for (const model of requiredModels) {
    const source = fs.readFileSync(path.join(root, model), "utf8");
    if (!source.includes("{{") || !source.includes("}}")) {
      throw new Error(`${model} should be a dbt model with ref/source macros`);
    }
  }

  if (await clickHouseReachable()) {
    await assertClickHouseTables();
    process.stdout.write("PASS ClickHouse table smoke\n");
  } else {
    process.stdout.write("SKIP ClickHouse table smoke: ClickHouse is not reachable on 127.0.0.1:8123\n");
  }
  process.stdout.write("PASS dbt model smoke\n");
}

async function clickHouseReachable() {
  try {
    const response = await fetch("http://127.0.0.1:8123/ping", {
      headers: clickHouseHeaders()
    });
    return response.ok && (await response.text()).trim() === "Ok.";
  } catch (_error) {
    return false;
  }
}

async function assertClickHouseTables() {
  const query = "SHOW TABLES FROM openeventflow FORMAT JSONEachRow";
  const response = await fetch(`http://127.0.0.1:8123/?query=${encodeURIComponent(query)}`, {
    headers: clickHouseHeaders()
  });
  if (!response.ok) {
    throw new Error(`ClickHouse table smoke failed: ${response.status} ${await response.text()}`);
  }
  const tables = new Set(
    (await response.text())
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line).name)
  );
  for (const table of [
    "ods_snowplow_enriched_events",
    "dwd_app_behavior_events",
    "fact_product_exposures",
    "fact_product_clicks",
    "fact_page_stays",
    "fact_cart_adds",
    "ads_product_behavior_daily",
    "fact_video_exposures",
    "fact_video_plays",
    "fact_video_watches",
    "fact_video_engagements",
    "ads_video_behavior_daily"
  ]) {
    if (!tables.has(table)) {
      throw new Error(`ClickHouse missing table ${table}`);
    }
  }
}

function clickHouseHeaders() {
  return {
    "x-clickhouse-user": process.env.CLICKHOUSE_USER || "openeventflow",
    "x-clickhouse-key": process.env.CLICKHOUSE_PASSWORD || "openeventflow"
  };
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 1;
});

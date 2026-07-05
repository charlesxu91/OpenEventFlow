const fs = require("node:fs");
const { createClickHouseAdapter, createClickHouseHttpClient, createWarehouseLoader } = require("./index");

function createWarehouseService(options = {}) {
  const adapter = options.adapter || createClickHouseAdapter({
    database: options.database || process.env.CLICKHOUSE_DATABASE || "openeventflow",
    client: options.client || createClickHouseHttpClient({
      endpoint: options.endpoint || process.env.CLICKHOUSE_ENDPOINT || "http://127.0.0.1:8123",
      username: options.username || process.env.CLICKHOUSE_USER || "openeventflow",
      password: options.password || process.env.CLICKHOUSE_PASSWORD || "openeventflow"
    })
  });
  const loader = options.loader || createWarehouseLoader({ adapter });

  return {
    loader,
    async loadEvents(events) {
      return loader.load(events);
    },
    async loadNdjson(ndjson) {
      return loader.load(parseNdjsonEvents(ndjson));
    }
  };
}

function parseNdjsonEvents(ndjson) {
  return ndjson
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function main() {
  const file = process.argv[2];
  const input = file ? fs.readFileSync(file, "utf8") : fs.readFileSync(0, "utf8");
  const service = createWarehouseService();
  const result = await service.loadNdjson(input);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = {
  createWarehouseService,
  parseNdjsonEvents
};

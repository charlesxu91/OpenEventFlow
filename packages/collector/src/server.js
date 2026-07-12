const fs = require("node:fs");
const path = require("node:path");
const {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createKafkaTopicBroker,
  createTrackingPlanRegistry
} = require("./index");

function createCollectorRuntime(options = {}) {
  const trackingPlanPath = options.trackingPlanPath || process.env.TRACKING_PLAN_PATH;
  if (!trackingPlanPath) throw new Error("trackingPlanPath or TRACKING_PLAN_PATH is required");
  const trackingPlan = JSON.parse(fs.readFileSync(path.resolve(trackingPlanPath), "utf8"));
  const broker = resolveBroker(options);
  const collector = createCollector({
    broker,
    registry: createTrackingPlanRegistry(trackingPlan),
    rawTopic: options.rawTopic || process.env.RAW_TOPIC || "snowplow_raw_events",
    validTopic: options.validTopic || process.env.VALID_TOPIC || "snowplow_enriched_events",
    badTopic: options.badTopic || process.env.BAD_TOPIC || "snowplow_bad_events"
  });
  const server = createHttpCollectorServer({
    collector,
    path: options.path || process.env.COLLECTOR_PATH || "/collect",
    maxBodyBytes: numberOption(options.maxBodyBytes, process.env.MAX_BODY_BYTES, 1024 * 1024),
    maxBatchSize: numberOption(options.maxBatchSize, process.env.MAX_BATCH_SIZE, 500),
    apiKeys: options.apiKeys || splitCsv(process.env.COLLECTOR_API_KEYS)
  });
  let closing;
  return {
    broker,
    collector,
    server,
    close() {
      if (!closing) {
        closing = new Promise((resolve, reject) => {
          server.close(async (error) => {
            try {
              await collector.close();
              if (error) reject(error); else resolve();
            } catch (closeError) { reject(closeError); }
          });
        });
      }
      return closing;
    }
  };
}

function resolveBroker(options = {}) {
  if (options.broker) return options.broker;
  const brokerType = options.brokerType || process.env.BROKER_TYPE;
  if (brokerType === "kafka") {
    return createKafkaTopicBroker({
      brokers: options.brokers || splitCsv(process.env.KAFKA_BROKERS),
      clientId: options.clientId || process.env.KAFKA_CLIENT_ID,
      compression: options.compression || process.env.KAFKA_COMPRESSION || "gzip"
    });
  }
  const adapterModule = options.brokerAdapterModule || process.env.BROKER_ADAPTER_MODULE;
  if (adapterModule) {
    const adapter = require(path.resolve(adapterModule));
    const broker = typeof adapter.createBroker === "function" ? adapter.createBroker() : adapter;
    if (!broker || typeof broker.publish !== "function") throw new Error("broker adapter must export publish() or createBroker()");
    return broker;
  }
  const durableRequired = options.requireDurableBroker === true || process.env.REQUIRE_DURABLE_BROKER === "true";
  if (durableRequired) throw new Error("durable broker required: set BROKER_TYPE=kafka or configure BROKER_ADAPTER_MODULE");
  if (brokerType && brokerType !== "memory") throw new Error(`unsupported broker type: ${brokerType}`);
  return createInMemoryTopicBroker();
}

async function main() {
  const port = Number(process.env.PORT || 8080);
  const host = process.env.HOST || "0.0.0.0";
  const runtime = createCollectorRuntime({
    trackingPlanPath: process.env.TRACKING_PLAN_PATH || path.join(__dirname, "../../../examples/tracking-plan.json")
  });
  runtime.server.listen(port, host, () => process.stdout.write(`openeventflow collector listening on http://${host}:${port}\n`));
  const shutdown = async (signal) => {
    process.stdout.write(`received ${signal}; shutting down collector\n`);
    try { await runtime.close(); }
    catch (error) {
      process.stderr.write(`${error.stack || error.message}\n`);
      process.exitCode = 1;
    }
  };
  process.once("SIGTERM", () => shutdown("SIGTERM"));
  process.once("SIGINT", () => shutdown("SIGINT"));
}

function splitCsv(value) {
  return String(value || "").split(",").map((item) => item.trim()).filter(Boolean);
}

function numberOption(explicit, environment, fallback) {
  const value = explicit === undefined ? environment : explicit;
  if (value === undefined) return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) throw new Error(`expected a positive integer, received ${value}`);
  return number;
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { createCollectorRuntime, resolveBroker };

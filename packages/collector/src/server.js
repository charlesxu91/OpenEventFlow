const fs = require("node:fs");
const path = require("node:path");
const {
  createCollector,
  createHttpCollectorServer,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry
} = require("./index");

function createCollectorRuntime(options = {}) {
  const trackingPlanPath = options.trackingPlanPath || process.env.TRACKING_PLAN_PATH;
  if (!trackingPlanPath) {
    throw new Error("trackingPlanPath or TRACKING_PLAN_PATH is required");
  }

  const trackingPlan = JSON.parse(fs.readFileSync(path.resolve(trackingPlanPath), "utf8"));
  const broker = options.broker || createInMemoryTopicBroker();
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
    apiKey: options.apiKey === undefined ? process.env.COLLECTOR_API_KEY : options.apiKey,
    maxBodyBytes: options.maxBodyBytes || Number(process.env.MAX_BODY_BYTES || 1024 * 1024),
    readiness: options.readiness
  });

  return { broker, collector, server };
}

async function main() {
  const port = Number(process.env.PORT || 8080);
  const host = process.env.HOST || "0.0.0.0";
  const runtime = createCollectorRuntime({
    trackingPlanPath: process.env.TRACKING_PLAN_PATH || path.join(__dirname, "../../../examples/tracking-plan.json")
  });
  runtime.server.listen(port, host, () => {
    process.stdout.write(`openeventflow collector listening on http://${host}:${port}\n`);
  });
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = {
  createCollectorRuntime
};

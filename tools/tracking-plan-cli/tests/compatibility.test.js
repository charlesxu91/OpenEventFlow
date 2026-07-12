const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");

function plan(events, overrides = {}) {
  return {
    namespace: "io.openeventflow.app",
    schemaVendor: "io.openeventflow",
    events,
    ...overrides
  };
}

function event(overrides = {}) {
  return {
    name: "recommendation_impression",
    version: "1-0-0",
    description: "An item was shown.",
    required: ["request_id"],
    properties: {
      request_id: { type: "string", description: "Request id." },
      model_version: { type: "string", description: "Model version." }
    },
    ...overrides
  };
}

test("additive events and optional fields are compatible and deterministic", () => {
  const { compareTrackingPlans } = require("../src/compatibility");
  const baseline = plan([event()]);
  const candidate = plan([
    event({
      properties: {
        request_id: { type: "string", description: "Request id." },
        model_version: { type: "string", description: "Model version." },
        experiment_id: { type: "string", description: "Experiment id." }
      }
    }),
    event({ name: "recommendation_click", description: "An item was clicked." })
  ]);

  assert.deepEqual(compareTrackingPlans(baseline, candidate), {
    compatible: true,
    changes: [
      {
        severity: "compatible",
        code: "FIELD_ADDED_OPTIONAL",
        path: "events.recommendation_impression.properties.experiment_id",
        before: null,
        after: { type: "string", description: "Experiment id." }
      },
      {
        severity: "compatible",
        code: "EVENT_ADDED",
        path: "events.recommendation_click",
        before: null,
        after: candidate.events[1]
      }
    ]
  });
});

test("retained deprecations are reported without making the plan incompatible", () => {
  const { compareTrackingPlans } = require("../src/compatibility");
  const candidateEvent = event({
    deprecated: { since: "2026-07-12", replacement: "recommendation_delivery", removeAfter: "2026-10-10" },
    properties: {
      request_id: { type: "string", description: "Request id." },
      model_version: {
        type: "string",
        description: "Model version.",
        deprecated: { since: "2026-07-12", replacement: "strategy_id", removeAfter: "2026-10-10" }
      }
    }
  });

  assert.deepEqual(compareTrackingPlans(plan([event()]), plan([candidateEvent])), {
    compatible: true,
    changes: [
      {
        severity: "deprecated",
        code: "EVENT_DEPRECATED",
        path: "events.recommendation_impression",
        before: null,
        after: candidateEvent.deprecated
      },
      {
        severity: "deprecated",
        code: "FIELD_DEPRECATED",
        path: "events.recommendation_impression.properties.model_version",
        before: null,
        after: candidateEvent.properties.model_version.deprecated
      }
    ]
  });
});

test("removed events and fields, type changes, and new required fields are breaking", () => {
  const { compareTrackingPlans } = require("../src/compatibility");
  const baseline = plan([
    event(),
    event({ name: "recommendation_click", description: "An item was clicked." })
  ]);
  const candidate = plan([
    event({
      required: ["request_id", "item_id"],
      properties: {
        request_id: { type: "integer", description: "Request id." },
        item_id: { type: "string", description: "Item id." }
      }
    })
  ]);
  const result = compareTrackingPlans(baseline, candidate);

  assert.equal(result.compatible, false);
  assert.deepEqual(result.changes.map(({ severity, code, path }) => ({ severity, code, path })), [
    {
      severity: "breaking",
      code: "FIELD_TYPE_CHANGED",
      path: "events.recommendation_impression.properties.request_id.type"
    },
    {
      severity: "breaking",
      code: "FIELD_REMOVED",
      path: "events.recommendation_impression.properties.model_version"
    },
    {
      severity: "breaking",
      code: "FIELD_ADDED_REQUIRED",
      path: "events.recommendation_impression.properties.item_id"
    },
    {
      severity: "breaking",
      code: "EVENT_REMOVED",
      path: "events.recommendation_click"
    }
  ]);
});

test("newly required fields remain breaking even when they declare defaults", () => {
  const { compareTrackingPlans } = require("../src/compatibility");
  const baseline = plan([event()]);
  const candidate = plan([event({
    required: ["request_id", "model_version", "item_id"],
    properties: {
      request_id: { type: "string" },
      model_version: { type: "string", default: "legacy" },
      item_id: { type: "string", default: "unknown" }
    }
  })]);

  const result = compareTrackingPlans(baseline, candidate);
  assert.equal(result.compatible, false);
  assert.deepEqual(result.changes.map(({ severity, code }) => ({ severity, code })), [
    { severity: "breaking", code: "FIELD_MADE_REQUIRED" },
    { severity: "breaking", code: "FIELD_ADDED_REQUIRED" }
  ]);
});

test("schema identity and event version changes are breaking", () => {
  const { compareTrackingPlans } = require("../src/compatibility");
  const result = compareTrackingPlans(
    plan([event()]),
    plan([event({ version: "2-0-0" })], { schemaVendor: "com.example" })
  );

  assert.equal(result.compatible, false);
  assert.deepEqual(result.changes.map((change) => change.code), [
    "SCHEMA_VENDOR_CHANGED",
    "EVENT_VERSION_CHANGED"
  ]);
});

test("compatibility CLI prints JSON and uses exit code 0 or 2", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "openeventflow-compatibility-"));
  const baselinePath = path.join(tempDir, "baseline.json");
  const candidatePath = path.join(tempDir, "candidate.json");
  fs.writeFileSync(baselinePath, JSON.stringify(plan([event()])));
  fs.writeFileSync(candidatePath, JSON.stringify(plan([event({ properties: {
    request_id: { type: "string" },
    model_version: { type: "string" },
    experiment_id: { type: "string" }
  } })])));

  const cliPath = path.resolve(__dirname, "../src/index.js");
  const compatible = spawnSync(process.execPath, [cliPath, "check-compatibility", baselinePath, candidatePath], {
    encoding: "utf8"
  });
  assert.equal(compatible.status, 0);
  assert.equal(JSON.parse(compatible.stdout).compatible, true);

  fs.writeFileSync(candidatePath, JSON.stringify(plan([event({ name: "replacement_event" })])));
  const breaking = spawnSync(process.execPath, [cliPath, "check-compatibility", baselinePath, candidatePath], {
    encoding: "utf8"
  });
  assert.equal(breaking.status, 2);
  assert.equal(JSON.parse(breaking.stdout).compatible, false);
});

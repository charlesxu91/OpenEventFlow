const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

test("generateArtifacts creates JSON Schema, Kotlin, and Swift files from a tracking plan", () => {
  const { generateArtifacts } = require("../src/generate");
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "openeventflow-codegen-"));
  const trackingPlan = {
    namespace: "io.openeventflow.app",
    schemaVendor: "io.openeventflow",
    events: [
      {
        name: "product_exposed",
        version: "1-0-0",
        owner: "recommendation",
        description: "Product exposure.",
        required: ["product_id", "position"],
        properties: {
          product_id: { type: "string", description: "Product id." },
          position: { type: "integer", description: "List position." },
          visible_ratio: { type: "number", description: "Visible ratio." }
        }
      }
    ]
  };

  const result = generateArtifacts(trackingPlan, tempDir);

  assert.deepEqual(result.generated.sort(), [
    "dart/open_event_flow_events.dart",
    "kotlin/OpenEventFlowEvents.kt",
    "schemas/io.openeventflow/product_exposed/jsonschema/1-0-0.json",
    "swift/OpenEventFlowEvents.swift",
    "typescript/openeventflow-events.ts"
  ]);

  const schemaPath = path.join(tempDir, "schemas/io.openeventflow/product_exposed/jsonschema/1-0-0.json");
  const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  assert.equal(schema.self.vendor, "io.openeventflow");
  assert.equal(schema.self.name, "product_exposed");
  assert.deepEqual(schema.required, ["product_id", "position"]);
  assert.equal(schema.properties.position.type, "integer");

  const kotlin = fs.readFileSync(path.join(tempDir, "kotlin/OpenEventFlowEvents.kt"), "utf8");
  assert.match(kotlin, /data class ProductExposedEvent/);
  assert.match(kotlin, /val productId: String/);
  assert.match(kotlin, /val position: Long/);
  assert.match(kotlin, /val visibleRatio: Double\?/);
  assert.match(kotlin, /const val PRODUCT_EXPOSED_SCHEMA/);

  const swift = fs.readFileSync(path.join(tempDir, "swift/OpenEventFlowEvents.swift"), "utf8");
  assert.match(swift, /public struct ProductExposedEvent/);
  assert.match(swift, /public let productId: String/);
  assert.match(swift, /public let position: Int/);
  assert.match(swift, /public let visibleRatio: Double\?/);
  assert.match(swift, /public static let productExposedSchema/);

  const typescript = fs.readFileSync(path.join(tempDir, "typescript/openeventflow-events.ts"), "utf8");
  assert.match(typescript, /export interface ProductExposedEvent/);
  assert.match(typescript, /productId: string;/);
  assert.match(typescript, /position: number;/);
  assert.match(typescript, /visibleRatio\?: number;/);
  assert.match(typescript, /export const PRODUCT_EXPOSED_SCHEMA/);

  const dart = fs.readFileSync(path.join(tempDir, "dart/open_event_flow_events.dart"), "utf8");
  assert.match(dart, /class ProductExposedEvent/);
  assert.match(dart, /final String productId;/);
  assert.match(dart, /final int position;/);
  assert.match(dart, /final double\? visibleRatio;/);
  assert.match(dart, /static const String productExposedSchema/);
});

test("generateArtifacts rejects invalid tracking plans before writing output", () => {
  const { generateArtifacts } = require("../src/generate");
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "openeventflow-invalid-"));

  assert.throws(
    () => generateArtifacts({ namespace: "io.openeventflow.app", events: [] }, tempDir),
    /schemaVendor is required/
  );
});

test("generateArtifacts validates and retains deprecation metadata", () => {
  const { generateArtifacts } = require("../src/generate");
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "openeventflow-deprecated-"));
  const eventDeprecation = {
    since: "2026-07-12",
    replacement: "recommendation_delivery",
    removeAfter: "2026-10-10"
  };
  const fieldDeprecation = {
    since: "2026-07-12",
    replacement: "strategy_id",
    removeAfter: "2026-10-10"
  };
  const trackingPlan = {
    namespace: "io.openeventflow.app",
    schemaVendor: "io.openeventflow",
    events: [{
      name: "recommendation_impression",
      version: "1-0-0",
      deprecated: eventDeprecation,
      properties: {
        model_version: { type: "string", deprecated: fieldDeprecation }
      }
    }]
  };

  generateArtifacts(trackingPlan, tempDir);
  const schema = JSON.parse(fs.readFileSync(path.join(
    tempDir,
    "schemas/io.openeventflow/recommendation_impression/jsonschema/1-0-0.json"
  ), "utf8"));

  assert.equal(schema.deprecated, true);
  assert.deepEqual(schema["x-openeventflow-deprecation"], eventDeprecation);
  assert.equal(schema.properties.model_version.deprecated, true);
  assert.deepEqual(schema.properties.model_version["x-openeventflow-deprecation"], fieldDeprecation);
});

test("validateTrackingPlan rejects malformed or too-short deprecation windows", () => {
  const { validateTrackingPlan } = require("../src/generate");
  const basePlan = {
    namespace: "io.openeventflow.app",
    schemaVendor: "io.openeventflow",
    events: [{
      name: "recommendation_impression",
      version: "1-0-0",
      properties: { request_id: { type: "string" } }
    }]
  };
  const invalidValues = [
    [true, /deprecated must be an object/],
    [{ since: "2026-07-12", replacement: "next" }, /removeAfter is required/],
    [{ since: "2026-02-30", replacement: "next", removeAfter: "2026-06-01" }, /since must be a valid YYYY-MM-DD date/],
    [{ since: "2026-07-12", replacement: "", removeAfter: "2026-10-10" }, /replacement is required/],
    [{ since: "2026-07-12", replacement: "next", removeAfter: "2026-10-09" }, /removeAfter must be at least 90 days after since/],
    [{ since: "2026-07-12", replacement: "next", removeAfter: "not-a-date" }, /removeAfter must be a valid YYYY-MM-DD date/],
    [{ since: "2026-07-12", replacement: "next", removeAfter: "2026-10-10", remove_after: "typo" }, /unsupported key remove_after/]
  ];

  for (const [deprecated, expected] of invalidValues) {
    const candidate = structuredClone(basePlan);
    candidate.events[0].properties.request_id.deprecated = deprecated;
    assert.throws(() => validateTrackingPlan(candidate), expected);
  }
});

test("example recommendation contracts share correlation identity fields", () => {
  const trackingPlan = JSON.parse(
    fs.readFileSync(path.resolve(__dirname, "../../../examples/tracking-plan.json"), "utf8")
  );
  const recommendationEvents = [
    "recommendation_delivery",
    "recommendation_impression",
    "recommendation_click",
    "recommendation_add_to_cart",
    "recommendation_payment",
    "recommendation_refund"
  ];

  for (const eventName of recommendationEvents) {
    const event = trackingPlan.events.find((candidate) => candidate.name === eventName);
    assert.ok(event, `${eventName} must exist`);
    for (const fieldName of ["request_id", "impression_id", "item_type", "item_id"]) {
      assert.equal(event.properties[fieldName].type, "string", `${eventName}.${fieldName}`);
      assert.ok(event.required.includes(fieldName), `${eventName}.${fieldName} must be required`);
    }
  }
});

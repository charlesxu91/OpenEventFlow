const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "../..");

test("open-source release surface includes service runners, platform build files, and deploy templates", () => {
  for (const file of [
    "packages/collector/src/server.js",
    "packages/recommendation/src/attribution.js",
    "streaming/flink-recommendation/pom.xml",
    "deploy/k8s/collector.yaml",
    "docs/recommendation-pipeline.md",
    "docs/schema-evolution.md",
    "packages/warehouse/src/service.js",
    "scripts/smoke-dbt-clickhouse.js",
    "deploy/snowplow/docker-compose.yml",
    "deploy/snowplow/README.md",
    "deploy/snowplow/config/iglu-resolver.json",
    "deploy/snowplow/config/enrichments/anon_ip.json",
    "sdks/android/settings.gradle.kts",
    "sdks/android/build.gradle.kts",
    "sdks/ios/Package.swift"
  ]) {
    assert.equal(fs.existsSync(path.join(root, file)), true, `${file} should exist`);
  }
});

test("release documentation indexes implemented recommendation and governance modules", () => {
  const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");
  for (const phrase of [
    "Recommendation attribution core",
    "Flink attribution and realtime-interest jobs",
    "Tracking-plan compatibility checks"
  ]) {
    assert.match(readme, new RegExp(phrase));
  }
});

test("production collector manifest forbids the in-memory broker", () => {
  const manifest = fs.readFileSync(path.join(root, "deploy/k8s/collector.yaml"), "utf8");
  assert.match(manifest, /name: REQUIRE_DURABLE_BROKER\s+value: "true"/);
});

test("npm packages expose a publishable file surface and package exports", () => {
  const packages = [
    "collector",
    "core",
    "react",
    "react-native",
    "snowplow-adapter",
    "warehouse",
    "web"
  ];

  for (const packageName of packages) {
    const packageJsonPath = path.join(root, "packages", packageName, "package.json");
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
    assert.deepEqual(packageJson.files, ["src", "README.md"], `${packageName} should publish src and README only`);
    assert.equal(packageJson.exports["."], "./src/index.js", `${packageName} should export its main entry`);
  }

  const cliPackageJson = JSON.parse(
    fs.readFileSync(path.join(root, "tools/tracking-plan-cli/package.json"), "utf8")
  );
  assert.equal(cliPackageJson.private, undefined, "tracking-plan CLI should be publishable");
  assert.deepEqual(cliPackageJson.files, ["src", "README.md"], "tracking-plan CLI should publish src and README only");
  assert.equal(cliPackageJson.bin["openeventflow-tracking-plan"], "src/index.js");
});

test("root scripts and CI cover node, mobile toolchains, dbt smoke, and docker e2e", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  for (const script of [
    "test",
    "test:mobile",
    "smoke:dbt",
    "smoke:docker",
    "verify"
  ]) {
    assert.equal(typeof packageJson.scripts[script], "string", `missing npm script ${script}`);
  }

  const ci = fs.readFileSync(path.join(root, ".github/workflows/ci.yml"), "utf8");
  for (const command of [
    "npm test",
    "npm run test:mobile",
    "npm run smoke:dbt",
    "npm run codegen"
  ]) {
    assert.match(ci, new RegExp(escapeRegExp(command)), `CI should run ${command}`);
  }
});

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { compareTrackingPlans } = require("./compatibility");
const { generateArtifacts, validateTrackingPlan } = require("./generate");

function main(argv) {
  const [, , command, firstPath, secondPath] = argv;

  if (command === "generate" && firstPath && secondPath) {
    const absolutePlanPath = path.resolve(firstPath);
    const absoluteOutputDir = path.resolve(secondPath);
    const trackingPlan = readJson(absolutePlanPath);
    const result = generateArtifacts(trackingPlan, absoluteOutputDir);

    for (const file of result.generated) {
      process.stdout.write(`generated ${file}\n`);
    }

    return 0;
  }

  if (command === "check-compatibility" && firstPath && secondPath) {
    const baseline = readJson(path.resolve(firstPath));
    const candidate = readJson(path.resolve(secondPath));
    validateTrackingPlan(baseline);
    validateTrackingPlan(candidate);
    const result = compareTrackingPlans(baseline, candidate);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return result.compatible ? 0 : 2;
  }

  printUsage();
  return 1;
}

function printUsage() {
  process.stderr.write([
    "Usage:",
    "  openeventflow-tracking-plan generate <tracking-plan.json> <output-dir>",
    "  openeventflow-tracking-plan check-compatibility <baseline.json> <candidate.json>",
    ""
  ].join("\n"));
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

if (require.main === module) {
  process.exitCode = main(process.argv);
}

module.exports = { main };

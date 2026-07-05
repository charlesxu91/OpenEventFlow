#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { generateArtifacts } = require("./generate");

function main(argv) {
  const [, , command, planPath, outputDir] = argv;

  if (command !== "generate" || !planPath || !outputDir) {
    printUsage();
    return 1;
  }

  const absolutePlanPath = path.resolve(planPath);
  const absoluteOutputDir = path.resolve(outputDir);
  const trackingPlan = JSON.parse(fs.readFileSync(absolutePlanPath, "utf8"));
  const result = generateArtifacts(trackingPlan, absoluteOutputDir);

  for (const file of result.generated) {
    process.stdout.write(`generated ${file}\n`);
  }

  return 0;
}

function printUsage() {
  process.stderr.write("Usage: openeventflow-tracking-plan generate <tracking-plan.json> <output-dir>\n");
}

if (require.main === module) {
  process.exitCode = main(process.argv);
}

module.exports = { main };

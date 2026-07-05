#!/usr/bin/env node

const { spawnSync } = require("node:child_process");

const checks = [
  {
    name: "Dart analyze",
    command: "dart",
    args: ["analyze", "sdks/flutter"],
    env: { HOME: "/private/tmp" }
  },
  {
    name: "Kotlin compile",
    command: "kotlinc",
    args: [
      "sdks/android/src/main/kotlin/io/openeventflow/mobile/Analytics.kt",
      "sdks/android/src/main/kotlin/io/openeventflow/mobile/OpenEventFlowClient.kt",
      "-d",
      "/private/tmp/openeventflow-android-sdk.jar"
    ]
  },
  {
    name: "Swift typecheck",
    command: "swiftc",
    args: [
      "-module-cache-path",
      "/private/tmp/openeventflow-swift-module-cache",
      "-typecheck",
      "sdks/ios/Sources/OpenEventFlow/Analytics.swift",
      "sdks/ios/Sources/OpenEventFlow/OpenEventFlowClient.swift"
    ]
  }
];

let failed = false;
for (const check of checks) {
  if (!commandExists(check.command)) {
    process.stdout.write(`SKIP ${check.name}: ${check.command} not found\n`);
    continue;
  }
  const result = spawnSync(check.command, check.args, {
    cwd: process.cwd(),
    env: { ...process.env, ...(check.env || {}) },
    encoding: "utf8"
  });
  if (result.stdout) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr) {
    process.stderr.write(result.stderr);
  }
  if (result.status !== 0) {
    failed = true;
    process.stderr.write(`FAIL ${check.name}: exit ${result.status}\n`);
  } else {
    process.stdout.write(`PASS ${check.name}\n`);
  }
}

process.exitCode = failed ? 1 : 0;

function commandExists(command) {
  const result = spawnSync("which", [command], { encoding: "utf8" });
  return result.status === 0;
}

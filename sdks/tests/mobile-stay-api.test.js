const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "../..");

test("native and flutter SDKs expose the shared stay-duration API contract", () => {
  const files = [
    "sdks/android/src/main/kotlin/io/openeventflow/mobile/OpenEventFlowClient.kt",
    "sdks/ios/Sources/OpenEventFlow/OpenEventFlowClient.swift",
    "sdks/flutter/lib/open_event_flow.dart"
  ];

  for (const file of files) {
    const source = fs.readFileSync(path.join(root, file), "utf8");
    for (const method of [
      "beginStay",
      "pauseStay",
      "resumeStay",
      "endStay",
      "switchStay",
      "cancelStay",
      "flushActiveStays"
    ]) {
      assert.match(source, new RegExp(method), `${file} should expose ${method}`);
    }
    for (const field of ["page_stay", "duration_ms", "stay_id", "exit_reason"]) {
      assert.match(source, new RegExp(field), `${file} should emit ${field}`);
    }
  }
});

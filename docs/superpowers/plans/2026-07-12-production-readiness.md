# Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a tested production-shaped collector, recommendation contract and attribution core, runnable Flink jobs, and tracking-plan compatibility governance.

**Architecture:** Keep contract governance in the existing Node tracking-plan CLI, add a dependency-free Node attribution domain package for fast deterministic tests, and wrap the same documented event/sample contracts in a Maven Flink module. Collector hardening stays behind injectable broker and authentication boundaries; Kubernetes examples configure those runtime contracts without claiming responsibility for external Kafka operations.

**Tech Stack:** Node.js 20 `node:test`, Java 17, Apache Flink 1.20, Maven, Kubernetes autoscaling/v2 and policy/v1.

## Global Constraints

- Branch is `fix/production-readiness`.
- New behavior is test-first and each slice must show a failing test before production changes.
- Recommendation identity uses `request_id`, `impression_id`, `item_type`, and `item_id`; product-only identity is not accepted.
- In-memory broker acknowledgment is never documented as durable Kafka acknowledgment.
- Persistent mobile queues, OpenTelemetry dashboards, and full privacy governance remain explicitly out of scope.
- No deployment-specific protobuf client is coupled into the Flink module; feature delivery uses an injectable sink boundary.

---

### Task 1: Tracking-plan compatibility and recommendation contracts

**Files:**
- Modify: `tools/tracking-plan-cli/src/index.js`
- Create: `tools/tracking-plan-cli/src/compatibility.js`
- Modify: `tools/tracking-plan-cli/tests/generate.test.js`
- Create: `tools/tracking-plan-cli/tests/compatibility.test.js`
- Modify: `examples/tracking-plan.json`
- Create: `docs/schema-evolution.md`

**Interfaces:**
- Produces: `compareTrackingPlans(baseline, candidate) -> { compatible, changes }`
- Produces CLI: `openeventflow-tracking-plan check-compatibility <baseline> <candidate>` with exit code 2 for breaking changes.
- Produces recommendation schemas sharing `request_id`, `impression_id`, `item_type`, and `item_id`.

- [ ] Write tests proving additive optional fields are compatible, deprecations are reported, removed fields/type changes/new required fields are breaking, and CLI exit codes are stable.
- [ ] Run `node --test tools/tracking-plan-cli/tests/compatibility.test.js` and confirm failure because the module and command do not exist.
- [ ] Implement deterministic change records with `severity`, `code`, `path`, `before`, and `after` fields.
- [ ] Extend the CLI parser to print JSON and set exit code 2 only when `compatible` is false.
- [ ] Add recommendation delivery, impression, click, cart, payment, and refund contracts with common correlation fields.
- [ ] Run tracking-plan CLI tests and regenerate checked-in artifacts.
- [ ] Commit as `feat: govern schema evolution`.

### Task 2: Attribution domain package

**Files:**
- Create: `packages/recommendation/package.json`
- Create: `packages/recommendation/src/index.js`
- Create: `packages/recommendation/src/attribution.js`
- Create: `packages/recommendation/src/interest.js`
- Create: `packages/recommendation/tests/attribution.test.js`
- Create: `packages/recommendation/tests/interest.test.js`
- Modify: `package.json`
- Create: `docs/recommendation-pipeline.md`

**Interfaces:**
- Produces: `createAttributionEngine({ attributionWindowMs, allowedLatenessMs, positiveActions })`.
- Consumes normalized events through `process(event)` and event-time progress through `advanceWatermark(timestamp)`.
- Produces arrays named `samples`, `corrections`, `lateEvents`, and `duplicates`.
- Produces: `createInterestProfile({ halfLifeMs }).apply(event).snapshot(at)`.

- [ ] Write tests for event-id deduplication, impression correlation, click/cart/payment labels, expired negative samples, refund corrections, and late-event routing.
- [ ] Run attribution tests and confirm failure because the package does not exist.
- [ ] Implement keyed impression state, conversion references, watermark expiry, and stable training-sample IDs.
- [ ] Write tests for decayed category, brand, price-bucket, content-type, and action signals.
- [ ] Implement deterministic exponential decay and snapshots without wall-clock dependencies.
- [ ] Add the package to root tests and document exact input/output contracts.
- [ ] Run recommendation tests and commit as `feat: add recommendation attribution core`.

### Task 3: Flink recommendation module

**Files:**
- Create: `streaming/flink-recommendation/pom.xml`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/model/Event.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/model/TrainingSample.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/AttributionFunction.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/AttributionJob.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/InterestAggregateFunction.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/RealtimeInterestJob.java`
- Create: `streaming/flink-recommendation/src/main/java/io/openeventflow/recommendation/FeatureSink.java`
- Create: `streaming/flink-recommendation/src/test/java/io/openeventflow/recommendation/AttributionFunctionTest.java`
- Create: `streaming/flink-recommendation/src/test/java/io/openeventflow/recommendation/InterestAggregateFunctionTest.java`
- Create: `streaming/flink-recommendation/README.md`

**Interfaces:**
- Consumes JSON events from configurable Kafka topic `snowplow_enriched_events`.
- Produces JSON training samples to `openeventflow_training_samples` and late events to `openeventflow_late_events`.
- Produces feature updates through `FeatureSink.write(InterestProfile profile)`.

- [ ] Create Maven test fixtures that assert deduplication, watermark expiry, positive/negative labels, refund corrections, and interest decay.
- [ ] Run `mvn -f streaming/flink-recommendation/pom.xml test` and confirm compilation fails because implementation classes are absent.
- [ ] Implement serializable event/sample models and pure attribution transition logic.
- [ ] Implement keyed process function with event-time timers, state TTL, duplicate state, and late-event side output.
- [ ] Implement job wiring with bounded out-of-orderness watermarks, checkpointing, restart strategy, Kafka sources, and sinks.
- [ ] Implement interest aggregation and injectable feature-sink boundary.
- [ ] Run Maven tests/package and commit as `feat: add flink recommendation jobs`.

### Task 4: Collector production hardening

**Files:**
- Modify: `packages/collector/src/index.js`
- Modify: `packages/collector/src/server.js`
- Modify: `packages/collector/tests/collector.test.js`
- Modify: `packages/collector/README.md`
- Create: `deploy/k8s/collector.yaml`

**Interfaces:**
- `broker.publish(topic, message)` may return a promise and is always awaited.
- `createHttpCollectorServer({ collector, path, apiKey, maxBodyBytes, readiness })` exposes `/healthz`, `/readyz`, and the collection path.
- Collection failures caused by broker publication return HTTP 503.

- [ ] Write HTTP tests for health/readiness, missing and invalid API keys, accepted valid keys, oversized bodies, awaited publish, and broker failure.
- [ ] Run collector tests and confirm the new cases fail for missing behavior.
- [ ] Await raw and routed publications and attach a typed unavailable error without hiding validation failures.
- [ ] Implement constant-time API-key comparison, bounded body reading, health/readiness routes, and 401/413/503 mappings.
- [ ] Thread environment configuration through `server.js`.
- [ ] Add Kubernetes Secret reference, probes, resources, Service, HPA, and PDB.
- [ ] Run collector tests and commit as `feat: harden collector runtime`.

### Task 5: Documentation, roadmap, and complete verification

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `ROADMAP.md`
- Modify: `docs/architecture.md`
- Modify: `docs/repository-structure.md`
- Modify: `e2e/tests/project-completeness.test.js`

**Interfaces:**
- Documentation distinguishes implemented capabilities from operational follow-up work.
- Root verification includes the recommendation Node package; Maven remains an explicit independent command.

- [ ] Add completeness tests for the new module, compatibility command, recommendation docs, and collector manifests.
- [ ] Run the completeness test and confirm it fails before documentation/index updates.
- [ ] Update architecture, repository map, release status, and roadmap with precise implemented/out-of-scope boundaries.
- [ ] Run `npm test`, `npm run codegen`, `npm run test:mobile`, and `npm run smoke:dbt`.
- [ ] Run `mvn -f streaming/flink-recommendation/pom.xml clean package`.
- [ ] Run `git diff --check` and searches that ensure roadmap statements no longer contradict implemented modules.
- [ ] Review `git diff`, commit as `docs: document production recommendation pipeline`, and leave the branch ready for manual push.


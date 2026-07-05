# Repository Structure

This repository is organized as a monorepo so SDKs, collector code, generated contracts, warehouse models, and e2e tests can evolve together.

```text
OpenEventFlow/
  .github/
    ISSUE_TEMPLATE/           GitHub issue templates
    workflows/                CI workflows
    pull_request_template.md  Pull request checklist
  deploy/
    docker/                   Redpanda and ClickHouse local stack
    k8s/                      Kubernetes manifests
    snowplow/                 Snowplow Collector, Enrich, and Iglu templates
  docs/                       Architecture and operating documentation
  e2e/
    src/                      Sample app and pipeline test helpers
    tests/                    App-to-warehouse e2e tests
  examples/                   Tracking plan and SDK usage examples
  mobile/generated/           Generated schemas and typed event models
  packages/
    core/                     Shared analytics runtime
    collector/                Event collector and validation
    react/                    React bindings
    react-native/             React Native bindings
    snowplow-adapter/         Snowplow self-describing event adapter
    warehouse/                Warehouse loader and ClickHouse adapter
    web/                      Browser SDK
  sdks/
    android/                  Android Kotlin SDK
    flutter/                  Flutter/Dart SDK
    ios/                      iOS Swift SDK
    tests/                    Cross-SDK API contract tests
  tools/
    tracking-plan-cli/        Schema and SDK type generator
  warehouse/
    dbt/                      dbt project for warehouse models
```

## Boundary Rules

- `packages/core` owns platform-neutral analytics behavior.
- Platform packages and SDKs should wrap `packages/core` concepts instead of redefining event semantics.
- `tools/tracking-plan-cli` owns code generation from tracking plans.
- `packages/collector` owns ingestion, validation, topic publishing, and bad-event routing.
- `packages/warehouse` owns stream consumption and warehouse writes.
- `warehouse/dbt` owns SQL model definitions.
- `e2e` owns cross-boundary proof that SDK events match warehouse output.

## Generated Files

`mobile/generated` contains checked-in generated examples so readers can inspect output without running codegen first. Regenerate them with:

```bash
npm run codegen
```

## Local Runtime Files

The published repository should not include local runtime state such as `.omx`, `work`, `outputs`, `node_modules`, `.dart_tool`, or Docker volumes.

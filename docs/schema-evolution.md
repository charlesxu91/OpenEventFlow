# Schema Evolution

OpenEventFlow tracking plans are versioned contracts. Producers and consumers must evaluate a candidate plan against the currently supported baseline before release:

```bash
node tools/tracking-plan-cli/src/index.js check-compatibility baseline.json candidate.json
```

The command writes a JSON result containing `compatible` and deterministic `changes`. Every change includes `severity`, `code`, `path`, `before`, and `after`. Exit status `0` means the candidate is compatible; status `2` means at least one breaking change was found. Invocation and file errors use status `1`.

## Change policy

Compatible changes include new events, optional fields, and fields made optional. Documentation-only edits do not produce compatibility records. Defaults are not currently validated or applied across generated SDKs, so every new required field and every optional field made required is conservatively classified as breaking even if it declares `default`.

Deprecations retain the event or field while adding `deprecated` metadata. They are reported with `deprecated` severity but remain compatible. Use an object so the migration is explicit:

```json
{
  "deprecated": {
    "since": "2026-07-12",
    "replacement": "strategy_id",
    "removeAfter": "2026-10-12"
  }
}
```

Breaking changes include removing an event or field, changing a field type, making an existing optional field required without a default, adding a required field without a default, changing an event version in place, or changing the plan namespace or schema vendor.

## Deprecation and migration

Published events and fields must remain available for at least 90 days after deprecation and for at least one documented consumer release cycle, whichever is longer. Owners must identify the replacement and the earliest removal date. Producers should dual-write when practical; consumers must accept both forms during the migration window and stop relying on the deprecated form before its removal date.

A breaking contract is released as a new schema identity/version after affected consumers have migrated. CI should compare the proposed plan with the production baseline and block exit status `2`; an intentional breaking release requires an explicit, reviewed baseline transition rather than bypassing the check.

# Contributing

Thanks for helping build OpenEventFlow.

## Development

Run tests before opening a pull request:

```bash
npm test
```

Regenerate example artifacts after changing tracking-plan behavior:

```bash
npm run codegen
```

## Design Principles

- Tracking plans are the source of truth.
- Business apps should depend on OpenEventFlow APIs, not Snowplow internals.
- Generated event types should make invalid events hard to create.
- Mobile SDKs must be conservative by default and explicit about autotracking.
- Bad events are product feedback, not noise to ignore.

## Pull Requests

Include:

- A concise problem statement
- Tests for changed behavior
- Documentation updates when public behavior changes
- Example tracking plan changes for new event modeling features

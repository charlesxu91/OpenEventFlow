# Maintainers

OpenEventFlow is intended to be maintained as an open-source behavior data infrastructure project.

## Maintainer Responsibilities

- Keep SDK event semantics stable across platforms.
- Review tracking-plan and schema changes as data contracts.
- Require tests for collector, warehouse, and SDK behavior changes.
- Keep local Docker smoke tests working for Redpanda and ClickHouse.
- Keep release notes clear when event contracts or generated outputs change.

## Release Checklist

Before tagging a release:

```bash
npm run codegen
npm run verify
docker compose -f deploy/docker/docker-compose.yml up -d
npm run smoke:docker
npm run smoke:docker:video
npm run smoke:dbt
```

Update `CHANGELOG.md` with user-facing changes, migration notes, and any schema compatibility notes.

# Snowplow Deployment Template

This directory is a production-shaped Snowplow reference template for teams that want OpenEventFlow SDK events to enter an official Snowplow Collector, Enrich, and Iglu validation path.

The checked-in OpenEventFlow Docker pipeline remains the deterministic local default. Use this template when you are ready to replace the project collector with Snowplow services:

```text
OpenEventFlow SDK
  -> Snowplow Collector
  -> Redpanda / Kafka raw topic
  -> Snowplow Enrich + Iglu resolver
  -> enriched topic / bad-events topic
  -> @openeventflow/warehouse
  -> ClickHouse
```

The compose file intentionally uses environment-controlled image tags so production adopters can pin versions that match their Snowplow support policy:

```bash
SNOWPLOW_COLLECTOR_VERSION=3.1.0 \
SNOWPLOW_ENRICH_VERSION=5.0.0 \
IGLU_SERVER_VERSION=0.12.0 \
docker compose -f deploy/snowplow/docker-compose.yml config
```

Before running in production, wire the Kafka broker addresses, topic names, Iglu API keys, and persistence store according to your infrastructure standard.

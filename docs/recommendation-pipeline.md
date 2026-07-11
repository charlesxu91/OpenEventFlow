# Recommendation data pipeline

OpenEventFlow is the client-event and attribution boundary for the recommendation
platform. Recommendation serving remains a separate latency-sensitive system.

```text
Web/mobile SDK
  -> OpenEventFlow Collector
  -> recsys.client-raw.v1
  -> schema validation and enrichment
  -> recsys.client-behavior.v1 / recsys.client-bad.v1
  -> Flink recommendation attribution
  -> recsys.training-samples.v1
  -> object-storage Parquet datasets
  -> TensorFlow training
```

Server-side delivery events and authoritative commerce events use the same
canonical OpenEventFlow envelope and enter the validated behavior topic through
an authenticated Collector or compatible backend producer. A browser or mobile client must never be treated as the source
of truth for payment, cancellation, refund, price, or net GMV.

## Attribution identity

Recommendation events carry the same correlation tuple:

`request_id + impression_id + product_id`

`delivery_id`, surface, position, candidate source, model version, feature-set
version, experiment assignment, and recommendation generation are retained as
provenance. `event_id` is the transport deduplication key.

## Event ownership

| Event | Producer | Meaning |
| --- | --- | --- |
| `recommendation_delivered` | recommendation server | candidate returned to the client |
| `product_impressed` | client SDK | item met the visible-area and duration policy |
| click/favorite/cart | client SDK plus backend reconciliation | user action |
| order/payment/cancel/refund | commerce backend or CDC | authoritative transaction label |

## Delivery guarantees

- The Collector returns HTTP 202 only after the Kafka producer receives all
  acknowledgements.
- Clients retain `event_id` across retries. Kafka and Flink may deliver/process
  at least once, so downstream deduplication remains mandatory.
- Invalid contracts are routed to `recsys.client-bad.v1`; they are not silently
  coerced into the valid stream.
- Flink uses event time, bounded out-of-orderness, managed state, checkpointing,
  timers, and state cleanup before it is horizontally scaled.
- Negative labels are emitted only when the attribution window closes. Payment
  and refund updates may revise earlier labels through versioned/upsert output.

## Local k3s profile

The Collector deployment in `deploy/k3s` connects to
`kafka.recsys-infra.svc.cluster.local:9092`. The local profile uses small data
and replica counts to validate behavior. Production sizing, authentication,
TLS, registry integration, rack placement, and retention are supplied by the
production overlay.

```bash
./scripts/k3s-collector-up.sh
./scripts/k3s-flink-recommendation-up.sh
```

# Reliability and failure handling

## Idempotent consumers

Each Kafka consumer persists an `event_id` in a PostgreSQL `processed_events` table. Duplicate deliveries are ignored with `ON CONFLICT DO NOTHING`.

## Inventory consistency

Inventory reservations and releases run inside PostgreSQL transactions. Reservation updates use a conditional stock update so concurrent requests cannot drive stock below zero. If a reservation fails, the transaction rolls back and the Saga emits `InventoryFailed`.

## Retry and DLQ

Unexpected consumer failures are retried through `orders.retry` up to `KAFKA_MAX_RETRIES` (default: 3). The original event is preserved for retry and the retry count is carried in `x-retry-count`. After the retry budget is exhausted, the event is written to `orders.DLQ` with source topic, partition, offset, error, timestamp, and payload metadata.

Business failures such as insufficient stock are not sent to the DLQ because they are expected Saga outcomes and are represented by domain events.

## Kubernetes

The hardened Kustomize overlay adds non-root execution, seccomp `RuntimeDefault`, dropped Linux capabilities, no privilege escalation, read-only root filesystems, CPU/memory requests and limits, liveness/readiness probes, PodDisruptionBudgets, and namespace-level network isolation.

Deploy the hardened configuration with:

```bash
kubectl apply -k k8s/overlays/hardened
```

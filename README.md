# E-Commerce Order Processing System

![CI](https://github.com/shashank460/ecommerce-order-processing-system/actions/workflows/ci.yml/badge.svg)

A distributed backend system demonstrating event-driven order processing, Saga compensation, idempotency, concurrency-safe inventory, Redis caching, Kafka messaging, gRPC, observability, Docker and Kubernetes.

## Architecture

See the Mermaid source in [`docs/architecture.mmd`](docs/architecture.mmd).

```text
Client -> API Gateway -> Product / Order services
                         |
                         +--> PostgreSQL
                         +--> Redis
                         +--> Kafka
                                |
                 +--------------+---------------+
                 |              |               |
             Inventory       Payment       Notification
                 |              |               |
                gRPC       PostgreSQL       PostgreSQL
```

## Services

- **API Gateway** — public HTTP entry point, JWT authentication, request correlation, metrics and Swagger UI.
- **Product Service** — product catalog and Redis caching.
- **Order Service** — order creation, idempotency and Saga orchestration.
- **Inventory Service** — stock reservation/release and concurrency-safe PostgreSQL transactions; exposes gRPC stock lookup.
- **Payment Service** — deterministic payment simulator with failure injection and durable idempotent event consumption.
- **Notification Service** — asynchronous notification consumer with durable idempotent event consumption.

## Core workflow

`OrderCreated -> InventoryReserved -> PaymentCompleted -> OrderConfirmed`

If payment fails: `PaymentFailed -> ReleaseInventory -> OrderCancelled`.

Kafka consumers use a PostgreSQL `processed_events` inbox table keyed by `eventId`. This makes business handlers safe against Kafka's normal at-least-once redelivery model. The project does **not** claim global exactly-once semantics for external side effects.

## Event contracts

Kafka events use a versioned envelope:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "version": 1,
  "occurredAt": "2026-08-22T10:00:00.000Z",
  "correlationId": "request-or-saga-id",
  "payload": {}
}
```

Schemas are kept under [`contracts/`](contracts/). `eventId`, event type, version, timestamp and correlation ID provide stable metadata for consumers, tracing and replay-safe processing. Schema changes should introduce a new version rather than silently changing a published payload.

## API documentation

Swagger UI is served by the gateway at:

`http://localhost:3000/api-docs`

Machine-readable OpenAPI 3.0 JSON:

`http://localhost:3000/api-docs.json`

## Run locally

Requirements: Docker Desktop and Docker Compose.

```bash
cp .env.example .env
docker compose up --build
```

Gateway: `http://localhost:3000`

Health: `http://localhost:3000/health`

Swagger: `http://localhost:3000/api-docs`

Prometheus: `http://localhost:9090`

Grafana: `http://localhost:3003`

## Example

```bash
curl -X POST http://localhost:3000/products \
  -H 'content-type: application/json' \
  -d '{"name":"Mechanical Keyboard","price":7999,"stock":20}'

curl -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -H 'Idempotency-Key: demo-order-001' \
  -d '{"userId":"demo-user","items":[{"productId":1,"quantity":2}]}'
```

## Engineering concepts demonstrated

- Event-driven architecture with KafkaJS
- Versioned Kafka event contracts
- At-least-once delivery with idempotent consumers / inbox pattern
- Saga orchestration and compensating actions
- Idempotency keys for safe order retries
- PostgreSQL transactions and concurrency-safe inventory updates
- Redis product caching
- gRPC internal communication
- JWT authentication
- Structured logging and correlation IDs
- Prometheus metrics
- OpenTelemetry tracing hooks
- Swagger/OpenAPI contract
- Docker Compose local infrastructure
- Kubernetes deployment manifests

## Observability

Prometheus metrics are exposed by the gateway at `/metrics`. Grafana provisioning includes an order-processing dashboard under `observability/grafana/dashboards/`.

For a typical distributed request, the same `x-request-id` / correlation ID should be followed from gateway → order service → Kafka event → inventory/payment consumer. A production deployment can export the OpenTelemetry spans to Jaeger, Tempo or another OTLP-compatible backend for a waterfall view.

## Consistency and failure semantics

This project intentionally uses **at-least-once event delivery with idempotent consumers**, not a claim of end-to-end exactly-once processing. Database state changes are protected with PostgreSQL transactions, while consumer inboxes prevent the same event ID from executing its business handler twice after Kafka redelivery. External side effects would require their own idempotency mechanism or transactional integration.

## Testing

The service packages use TypeScript and Vitest. Run the available tests with:

```bash
npm install
npm test
```

GitHub Actions runs the test suite and validates the Docker Compose configuration on pushes and pull requests. The current repository contains unit-level coverage for idempotency validation; integration tests that exercise the full Kafka/PostgreSQL workflow should be added before claiming end-to-end concurrency coverage.

## Project status

This is a portfolio/learning project designed to demonstrate distributed backend engineering. Production hardening such as managed Kafka, managed PostgreSQL, secrets management, TLS termination and autoscaling should be configured before a real production deployment.

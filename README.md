# E-Commerce Order Processing System

A distributed backend system demonstrating event-driven order processing, Saga compensation, idempotency, concurrency-safe inventory, Redis caching, Kafka messaging, gRPC, observability, Docker and Kubernetes.

## Architecture

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
                 |
                gRPC
```

## Services

- **API Gateway** — public HTTP entry point, JWT authentication and request correlation.
- **Product Service** — product catalog and Redis caching.
- **Order Service** — order creation, idempotency and Saga orchestration.
- **Inventory Service** — stock reservation/release and concurrency-safe PostgreSQL transactions; exposes gRPC stock lookup.
- **Payment Service** — deterministic payment simulator with success/failure events.
- **Notification Service** — asynchronous order notification consumer.

## Core workflow

`OrderCreated -> InventoryReserved -> PaymentCompleted -> OrderConfirmed`

If payment fails: `PaymentFailed -> ReleaseInventory -> OrderCancelled`.

## Run locally

Requirements: Docker Desktop and Docker Compose.

```bash
cp .env.example .env
docker compose up --build
```

Gateway: `http://localhost:3000`

Health: `http://localhost:3000/health`

## Example

```bash
curl -X POST http://localhost:3000/products \
  -H 'content-type: application/json' \
  -d '{"name":"Mechanical Keyboard","price":7999,"stock":20}'

curl -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-order-001' \
  -d '{"userId":"demo-user","items":[{"productId":1,"quantity":2}]}'
```

## Engineering concepts demonstrated

- Event-driven architecture with KafkaJS
- Saga orchestration and compensating actions
- Idempotency keys for safe retries
- PostgreSQL row locking for inventory concurrency
- Redis product caching
- gRPC internal communication
- JWT authentication
- Structured logging and correlation IDs
- Prometheus metrics
- OpenTelemetry tracing hooks
- Docker Compose local infrastructure
- Kubernetes deployment manifests

## Observability

Prometheus metrics are exposed by the gateway at `/metrics`. The repository includes a Prometheus configuration and Grafana provisioning directory for local monitoring.

## Testing

The service packages use TypeScript and Vitest. Run the available tests with:

```bash
npm install
npm test
```

## Project status

This is a portfolio/learning project designed to demonstrate distributed backend engineering. Production hardening such as managed Kafka, managed PostgreSQL, secrets management, TLS termination and autoscaling should be configured before a real production deployment.

# AP2 Assignments 1–4 — Microservices in Go

**Student:** Chingiz Uraimov  
**Group:** SE-2405

---

## System Evolution Across Assignments

```mermaid
timeline
    title What was added in each assignment
    Assignment 1 : Order Service (REST + PostgreSQL)
    Assignment 2 : Payment Service (gRPC)
               : gRPC call from Order → Payment
    Assignment 3 : RabbitMQ message broker
               : Notification Service (consumer)
               : Dead Letter Queue
    Assignment 4 : Redis caching (cache-aside, TTL 5min)
               : Redis rate limiter (10 req/min/IP)
               : Adapter Pattern (EmailSender interface)
               : Exponential backoff (2s→4s→8s→16s→32s)
               : Parallel goroutines (prefetch 10)
               : Redis idempotency (notif:sent:{id})
```

---

## Full Architecture (Assignment 4)

```mermaid
flowchart TD
    Client([Client]):::client

    Client -->|"REST :9080"| RL

    subgraph OS["Order Service"]
        RL["Rate Limiter\n10 req/min/IP\n— added A4 —"]:::a4
        OH["HTTP Handler\n— added A1 —"]:::a1
        OUC["Order Use Case\n— added A1 —"]:::a1
        CACHE["CachedOrderRepo\ncache-aside decorator\n— added A4 —"]:::a4
        ODB[("Orders DB\nPostgreSQL\n— added A1 —")]:::a1

        RL --> OH --> OUC --> CACHE
        CACHE -->|"HIT → return"| OH
        CACHE -->|"MISS → query"| ODB
        CACHE -->|"UpdateStatus → DEL key"| CACHE
    end

    subgraph RC["Redis :6379 — added A4"]
        RK["order:{id} — cached order\nrate:{ip} — request counter\nnotif:sent:{id} — idempotency"]:::a4
    end

    CACHE <-->|"GET / SET / DEL"| RC
    RL <-->|"INCR / EXPIRE"| RC

    OUC -->|"gRPC ProcessPayment\n— added A2 —"| PS

    subgraph PS["Payment Service :9081 / :9082"]
        PH["gRPC Handler\n— added A2 —"]:::a2
        PUC["Payment Use Case\n— added A2 —"]:::a2
        PDB[("Payments DB\nPostgreSQL\n— added A2 —")]:::a2
        PUB["RabbitMQ Publisher\n— added A3 —"]:::a3

        PH --> PUC --> PDB
        PUC --> PUB
    end

    PUB -->|"PaymentEvent\npayment.completed"| RMQ

    RMQ{{"RabbitMQ :5672\n— added A3 —"}}:::a3

    subgraph NS["Notification Service"]
        CON["Consumer\nmanual ACK, prefetch 10\n— added A3 —"]:::a3
        GR["go func per message\nparallel goroutines\n— added A4 —"]:::a4
        WRK["Worker\nidempotency + backoff\n— added A4 —"]:::a4
        PROV["SimulatedProvider\nEmailSender interface\nAdapter Pattern\n— added A4 —"]:::a4

        CON --> GR --> WRK --> PROV
        WRK <-->|"notif:sent:{id}"| RC
    end

    RMQ -->|"Consume"| CON
    CON -->|"ACK on success"| RMQ
    CON -->|"NACK after 5 retries"| DLQ

    DLQ[("Dead Letter Queue\npayment.dead-letter\n— added A3 —")]:::a3

    classDef client fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef a1 fill:#f3f4f6,stroke:#6b7280,color:#111827
    classDef a2 fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef a3 fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef a4 fill:#dcfce7,stroke:#16a34a,color:#14532d
```

**Legend:** grey = A1 · purple = A2 · orange = A3 · green = A4

---

## Component Map

| Component | File | Added in |
|-----------|------|----------|
| Order HTTP handler | `order-service/internal/transport/http/handler.go` | A1 |
| Order use case | `order-service/internal/usecase/order.go` | A1 |
| Orders PostgreSQL repo | `order-service/internal/repository/postgres.go` | A1 |
| Payment gRPC handler | `payment-service/internal/transport/grpc/handler.go` | A2 |
| Payment gRPC client | `order-service/internal/repository/payment_grpc_client.go` | A2 |
| RabbitMQ publisher | `payment-service/internal/infrastructure/rabbitmq/publisher.go` | A3 |
| RabbitMQ consumer | `notification-service/internal/consumer/rabbitmq.go` | A3 |
| Dead Letter Queue | declared in `consumer/rabbitmq.go` | A3 |
| Redis cache decorator | `order-service/internal/cache/order_cache.go` | A4 |
| Rate limiter middleware | `order-service/internal/transport/http/middleware/rate_limiter.go` | A4 |
| EmailSender interface | `notification-service/internal/provider/provider.go` | A4 |
| SimulatedProvider | `notification-service/internal/provider/simulated.go` | A4 |
| Worker (backoff + idempotency) | `notification-service/internal/worker/worker.go` | A4 |
| Parallel goroutines | `notification-service/internal/consumer/rabbitmq.go:110` | A4 |

---

## Cache-Aside Pattern (Assignment 4)

```
GET /orders/:id
  → Check Redis "order:{id}"
  → HIT:  return cached JSON               ← no DB query
  → MISS: query PostgreSQL
          → write to Redis TTL 5 min
          → return result

UpdateStatus (after payment / cancel)
  → UPDATE orders SET status = ...
  → DEL "order:{id}"                       ← immediate invalidation
```

---

## Rate Limiter (Assignment 4 — Bonus +10%)

```
key = "rate:{client_ip}"

INCR key → count
if count == 1 → EXPIRE key 60s
if count > 10 → HTTP 429 Too Many Requests
```

Config: `RATE_LIMIT_MAX=10`, `RATE_LIMIT_WINDOW_SECONDS=60`

---

## Notification Reliability (Assignment 4)

### Idempotency
```
key = "notif:sent:{payment_id}"
EXISTS? YES → skip (duplicate)
        NO  → send → SET key EX 86400
```

### Exponential Backoff
```
attempt 1 → FAIL → sleep 2s
attempt 2 → FAIL → sleep 4s
attempt 3 → FAIL → sleep 8s
attempt 4 → FAIL → sleep 16s
attempt 5 → FAIL → NACK → Dead Letter Queue
```

### Parallel Processing
Each RabbitMQ message spawns a goroutine (`go func()`).  
Prefetch = 10 — up to 10 messages in-flight simultaneously.

---

## How to Run

```bash
DOCKER_BUILDKIT=0 docker-compose up --build
```

| Service | URL |
|---------|-----|
| Order Service REST | http://localhost:9080 |
| Payment Service REST | http://localhost:9081 |
| RabbitMQ Management | http://localhost:15672 (guest/guest) |
| Redis | localhost:6379 |

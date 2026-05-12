# AP2 Assignment 4 — Performance Optimization & External Integrations

**Student:** Chingiz Uraimov  
**Group:** SE-2405  

## Repository Links
- **Proto Repository:** https://github.com/Hanulus/ap2-protos
- **Generated Code Repository:** https://github.com/Hanulus/ap2-generated

---

## Architecture (Assignment 4)

```mermaid
flowchart TD
    Client([🧑 Client]):::client

    Client -->|"POST /orders\nGET /orders/:id\n(REST)"| RL

    subgraph OS["Order Service :9080"]
        RL["Rate Limiter\nMiddleware\n(Redis — 10 req/min)"]:::redis
        OH["HTTP Handler"]
        OUC["Order Use Case"]
        CACHE["CachedOrderRepo\n(Cache-aside decorator)"]:::redis
        ODB[("Orders DB\nPostgreSQL")]

        RL --> OH --> OUC --> CACHE
        CACHE -->|"1. HIT → return cached"| OH
        CACHE -->|"2. MISS → query DB\nthen write to Redis (TTL 5min)"| ODB
        CACHE -->|"UpdateStatus →\nDEL cache key"| CACHE
    end

    subgraph RC["Redis :6379"]
        RK["order:{id} — cached order\nrate:{ip} — request counter\nnotif:sent:{payment_id} — idempotency"]:::redis
    end

    OUC -->|"gRPC ProcessPayment"| PS

    subgraph PS["Payment Service :9081/:9082"]
        PDB[("Payments DB\nPostgreSQL")]
        PUB["RabbitMQ Publisher"]
        PS --> PDB
        PS --> PUB
    end

    PUB -->|"PaymentEvent JSON\npayment.completed"| RMQ

    subgraph NS["Notification Service"]
        CON["RabbitMQ Consumer\n(manual ACK)"]
        WRK["Worker\n(background)"]:::worker
        PROV["SimulatedProvider\n(EmailSender interface)"]

        CON --> WRK
        WRK -->|"1. Check Redis idempotency\n2. Send via provider\n3. Retry on error (exp. backoff)"| PROV
        WRK -->|"Mark notif:sent:{payment_id}"| RC
    end

    RMQ{{"RabbitMQ Broker\n:5672"}}
    DLQ[("Dead Letter Queue\npayment.dead-letter")]

    RMQ -->|"Consume"| CON
    CON -->|"ACK after success"| RMQ
    CON -->|"NACK — all retries exhausted"| DLQ

    CACHE <-->|"GET / SET / DEL"| RC
    RL <-->|"INCR / EXPIRE"| RC

    classDef client fill:#dbeafe,stroke:#3b82f6,color:#1e3a8a
    classDef redis fill:#fef9c3,stroke:#eab308,color:#713f12
    classDef worker fill:#dcfce7,stroke:#22c55e,color:#14532d
```

---

## What Changed from Assignment 3

| Feature | Assignment 3 | Assignment 4 |
|---|---|---|
| Order caching | No cache | Redis cache-aside, TTL 5 min |
| Cache invalidation | — | DEL on status update |
| Rate limiting | — | Redis INCR, 10 req/min/IP |
| Notification | In-memory idempotency, fake log | Redis idempotency + real provider interface |
| Retry strategy | RabbitMQ requeue (fixed attempts) | Exponential backoff inside Worker (2s→4s→8s) |
| Email provider | Hardcoded log | Swappable via `PROVIDER_MODE` env var |

---

## Cache Invalidation Strategy

**Pattern:** Cache-aside (lazy loading)

**Read path:**
```
GET /orders/:id
  → Check Redis key "order:{id}"
  → HIT:  return cached JSON (no DB query)
  → MISS: query PostgreSQL → write to Redis with TTL 5 min → return result
```

**Write path (invalidation):**
```
UpdateStatus (called after payment or cancellation)
  → UPDATE orders SET status = ... WHERE id = ...
  → DEL "order:{id}"   ← atomic: cache deleted right after DB write
```

Deleting (rather than updating) the key guarantees that stale data is never served — the next read will fetch fresh data from the DB and re-populate the cache.

**TTL** acts as a safety net: even if invalidation is somehow missed (e.g. direct DB edit), the cache expires in 5 minutes.

---

## Retry & Idempotency Strategy

### Idempotency (Notification Service)

Before sending any notification, the Worker checks Redis:
```
key = "notif:sent:{payment_id}"

EXISTS key?
  YES → log "DUPLICATE skipped", return nil  ← no email sent
  NO  → proceed to send
        on success → SET key "sent" EX 86400 (24h TTL)
```

This ensures **exactly-once delivery** even when RabbitMQ re-delivers the same message (at-least-once guarantee).

### Exponential Backoff (Notification Worker)

If the provider returns an error, the worker retries with increasing delays:

```
attempt 1 → send → FAIL → sleep 2s
attempt 2 → send → FAIL → sleep 4s
attempt 3 → send → FAIL → give up
  → return error to consumer → NACK → DLQ
```

Formula: `delay = 2s * 2^(attempt-1)` — doubles each time.

The SimulatedProvider fails ~30% of calls randomly, so you can observe retries in the logs:
```
[Worker] FAIL payment_id=... attempt=1/3 err=transient network error, retrying in 2s
[Worker] SUCCESS payment_id=... attempt=2
```

---

## Bonus: Rate Limiter (+10%)

A Redis-based rate limiter middleware is applied globally to the Order Service.

**Logic:**
```
key = "rate:{client_ip}"

INCR key → count
if count == 1: EXPIRE key 60s   ← start the 1-minute window
if count > 10: return HTTP 429 Too Many Requests
```

Configuration via env vars:
- `RATE_LIMIT_MAX=10` — max requests per window
- `RATE_LIMIT_WINDOW_SECONDS=60` — window size in seconds

If Redis is unavailable, the middleware lets requests through (fail-open) to avoid blocking the entire API.

---

## How to Run

### Prerequisites
- Docker & Docker Compose

### Start all services
```bash
docker-compose up --build
```

This starts:
- `orders-db` — PostgreSQL for orders
- `payments-db` — PostgreSQL for payments
- `redis` — Redis :6379
- `rabbitmq` — RabbitMQ (management UI at http://localhost:15672, guest/guest)
- `payment-service` — REST :9081, gRPC :9082
- `order-service` — REST :9080 (with caching + rate limiter)
- `notification-service` — background worker

### Create an order
```bash
curl -X POST http://localhost:9080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"cust-1","item_name":"Book","amount":500}'
```

### Get order (cache-aside in action)
```bash
# First call: MISS → DB query → cached in Redis
curl http://localhost:9080/orders/{id}

# Second call: HIT → served from Redis
curl http://localhost:9080/orders/{id}
```

Watch order-service logs:
```
[Cache] MISS order:xxxxxxxx
[Cache] HIT  order:xxxxxxxx
```

### Test rate limiter
```bash
# Run 11 times rapidly — the 11th returns HTTP 429
for i in {1..11}; do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:9080/orders/test; done
```

### Test exponential backoff
Watch notification-service logs after creating an order — you'll see retries when the simulated provider fails:
```
[Worker] FAIL payment_id=... attempt=1/3, retrying in 2s
[Worker] SUCCESS payment_id=... attempt=2
```

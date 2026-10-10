# Sluice

A small event-driven transaction ingestion gateway built in Node.js.

## Run

Start the services with Docker Compose:

```bash
docker compose up -d
```

Install dependencies and start the gateway and worker:

```bash
npm install
npm run gateway
npm run worker
```

Run tests:

```bash
npm test
```

## Request Flow

1. Client sends `POST /transactions` with `x-tenant-id` and `x-idempotency-key`.
2. Redis checks token bucket rate limit using Redis server `TIME` in Lua.
3. Redis checks idempotency key and SHA-256 body hash atomically.
4. Gateway publishes payload to Kafka topic `transactions` with `acks=-1`.
5. Gateway replies `202 Accepted` immediately with queued status.
6. Worker consumes Kafka batches, locks accounts in Postgres, and commits offsets after write.

## Guarantees

- **Delivery**: At-least-once. Messages are not lost if workers crash or fail.
- **Deduplication**: Duplicates are prevented by a unique constraint on `(tenant_id, idempotency_key)` in Postgres (`ON CONFLICT DO NOTHING`). The financial effect is processed once. We do not claim exactly-once delivery.
- **Publish Failure**: If Kafka publish fails, the Redis reservation is deleted and `503` is returned so the client can retry.
- **Broker Buffering**: When Postgres was stopped for 30 seconds during a load test, no messages were lost; the gateway kept returning `202`, and the worker resumed writing once Postgres was restarted.

## Known Limits

- **DLQ Ordering**: Poisoned messages that fail 5 times are sent to `transactions.dlq`. This skips the failed message and breaks FIFO per-account ordering for subsequent transactions on that account.
- **Local Single Broker**: Docker Compose runs a single broker with `replication-factor=1` and `min.insync.replicas=1`. Multi-broker replication cannot be demonstrated on one local node.
- **TTL vs Lag**: The idempotency key TTL in Redis defaults to 24 hours (`IDEMPOTENCY_TTL_SECONDS=86400`). If worker consumer lag exceeds this TTL, duplicate requests arriving after key expiry could be re-published to Kafka.
- **Single-Threaded Node**: The gateway runs as a single Node.js process on one thread. Throughput is bounded by event-loop single-core CPU execution.

## Measured Benchmark

Measured with k6 running 100 concurrent virtual users over 40 seconds against a single Node.js gateway process. All services (Postgres 15, Redis 7, Kafka 3.7, Gateway, Worker, and k6) were running on the same host machine.

### Machine Specs
- **CPU**: 12th Gen Intel Core i5-12450H (8 cores, 12 logical processors)
- **RAM**: 16 GB
- **OS**: Windows 11 / WSL2
- **Process Count**: 1 Node.js gateway process (single thread)

### Results

| Metric | Measured Value |
|---|---|
| Total Requests | 102,617 |
| Success Rate | 100.00% (0 errors) |
| Requests / Second | 2,565 req/s |
| p50 Latency | 21.91 ms |
| p90 Latency | 29.30 ms |
| p95 Latency | 32.83 ms |
| Max Latency | 90.58 ms |

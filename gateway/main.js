import crypto from 'node:crypto';
import Fastify from 'fastify';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { allowRequest } from './limit.js';
import { alreadySeen, markDone, forgetKey } from './seen.js';
import { publish } from './publish.js';

const PORT = Number(process.env.PORT || 3000);
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = process.env.TOPIC || 'transactions';
const RATE_CAPACITY = Number(process.env.RATE_CAPACITY || 100);
const RATE_PER_SECOND = Number(process.env.RATE_PER_SECOND || 50);

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({ clientId: 'gateway', brokers: KAFKA_BROKERS });
const producer = kafka.producer();

const app = Fastify();

app.post('/transactions', async (req, reply) => {
  const tenantId = req.headers['x-tenant-id'] || req.body?.tenant_id;
  const idempotencyKey = req.headers['x-idempotency-key'] || req.body?.idempotency_key;

  if (!tenantId || !idempotencyKey || !req.body?.account_id || req.body?.amount === undefined) {
    return reply.status(400).send({ error: 'missing required fields' });
  }

  const limit = await allowRequest(redis, tenantId, RATE_CAPACITY, RATE_PER_SECOND);
  if (!limit.allowed) {
    reply.header('Retry-After', limit.retryAfter);
    return reply.status(429).send({ error: 'rate limit exceeded' });
  }

  const bodySha = crypto.createHash('sha256').update(JSON.stringify(req.body)).digest('hex');
  const transactionId = crypto.randomUUID();
  const initialResponse = { status: 'queued', id: transactionId };

  const seen = await alreadySeen(redis, tenantId, idempotencyKey, bodySha, initialResponse);
  if (seen.seen) {
    if (!seen.match) {
      return reply.status(422).send({ error: 'idempotency key reused with different body' });
    }
    return reply.status(202).send(seen.response);
  }

  const message = {
    id: transactionId,
    tenant_id: tenantId,
    account_id: req.body.account_id,
    amount: Number(req.body.amount),
    idempotency_key: idempotencyKey,
    created_at: new Date().toISOString()
  };

  try {
    await publish(producer, TOPIC, message);
    await markDone(redis, tenantId, idempotencyKey, initialResponse);
    return reply.status(202).send(initialResponse);
  } catch (err) {
    console.error('publish failed:', err.message);
    // remove key from redis so client can safely retry
    await forgetKey(redis, tenantId, idempotencyKey);
    return reply.status(503).send({ error: 'service temporarily unavailable' });
  }
});

async function start() {
  try {
    await producer.connect();
    await app.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`gateway listening on ${PORT}`);
  } catch (err) {
    console.error('gateway startup failed:', err.message);
    process.exit(1);
  }
}

start();

export { app, redis, producer };

import test from 'node:test';
import assert from 'node:assert';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import pg from 'pg';
import { Kafka } from 'kafkajs';

const PG_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/sluice';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = process.env.TOPIC || 'transactions';
const PORT = 3000;

function stopProcess(proc) {
  if (proc && proc.pid) {
    try {
      if (process.platform === 'win32') {
        spawn('taskkill', ['/pid', String(proc.pid), '/f', '/t']);
      } else {
        proc.kill('SIGKILL');
      }
    } catch {}
  }
}

test('10,000 concurrent requests with one key produce 1 kafka message and 1 postgres row', async () => {
  const pool = new pg.Pool({ connectionString: PG_URL });

  const gateway = spawn('node', ['gateway/main.js'], {
    env: {
      ...process.env,
      PORT: String(PORT),
      TOPIC,
      RATE_CAPACITY: '25000',
      RATE_PER_SECOND: '25000'
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  const worker = spawn('node', ['worker/main.js'], {
    env: {
      ...process.env,
      TOPIC,
      FROM_BEGINNING: 'true',
      GROUP_ID: `test-worker-${Date.now()}`
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  gateway.stdout.on('data', d => console.log('gateway stdout:', d.toString()));
  gateway.stderr.on('data', d => console.error('gateway stderr:', d.toString()));
  worker.stdout.on('data', d => console.log('worker stdout:', d.toString()));
  worker.stderr.on('data', d => console.error('worker stderr:', d.toString()));

  const kafka = new Kafka({ clientId: 'test-inspector', brokers: KAFKA_BROKERS });
  const consumer = kafka.consumer({ groupId: `inspector-${Date.now()}` });

  try {
    let gatewayReady = false;
    for (let i = 0; i < 30; i++) {
      try {
        await fetch(`http://127.0.0.1:${PORT}/transactions`, { method: 'POST' });
        gatewayReady = true;
        break;
      } catch {
        await new Promise(r => setTimeout(r, 400));
      }
    }
    assert.ok(gatewayReady, 'gateway failed to start');

    const tenantId = `tenant_${Date.now()}`;
    const accountId = `acc_${Date.now()}`;
    const idempotencyKey = `key_${crypto.randomUUID()}`;
    const body = {
      tenant_id: tenantId,
      account_id: accountId,
      amount: 100,
      idempotency_key: idempotencyKey
    };

    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    const kafkaMessages = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const val = JSON.parse(message.value.toString());
          if (val.tenant_id === tenantId && val.idempotency_key === idempotencyKey) {
            kafkaMessages.push(val);
          }
        } catch {}
      }
    });

    console.log('sending 10,000 concurrent requests...');
    const promises = [];
    for (let i = 0; i < 10000; i++) {
      promises.push(
        fetch(`http://127.0.0.1:${PORT}/transactions`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-idempotency-key': idempotencyKey
          },
          body: JSON.stringify(body)
        })
      );
    }

    const responses = await Promise.all(promises);
    const status202Count = responses.filter(r => r.status === 202).length;
    assert.strictEqual(status202Count, 10000, 'every response must return 202');

    // poll until worker finishes processing kafka batch
    let txCount = 0;
    for (let i = 0; i < 30; i++) {
      const txRes = await pool.query(
        'SELECT count(*)::int as count FROM transactions WHERE tenant_id = $1 AND idempotency_key = $2',
        [tenantId, idempotencyKey]
      );
      txCount = txRes.rows[0].count;
      if (txCount > 0) break;
      await new Promise(r => setTimeout(r, 500));
    }

    assert.strictEqual(kafkaMessages.length, 1, `expected exactly 1 kafka message, received ${kafkaMessages.length}`);
    assert.strictEqual(txCount, 1, `expected exactly 1 transaction in postgres, found ${txCount}`);

    const keyRes = await pool.query(
      'SELECT count(*)::int as count FROM idempotency_keys WHERE tenant_id = $1 AND idempotency_key = $2',
      [tenantId, idempotencyKey]
    );
    assert.strictEqual(keyRes.rows[0].count, 1, `expected exactly 1 idempotency key in postgres, found ${keyRes.rows[0].count}`);

    const accRes = await pool.query(
      'SELECT balance::int as balance FROM accounts WHERE tenant_id = $1 AND account_id = $2',
      [tenantId, accountId]
    );
    assert.strictEqual(accRes.rows[0].balance, 100, `expected account balance to be 100, found ${accRes.rows[0].balance}`);

    console.log('all assertions passed successfully');
  } finally {
    try { await consumer.disconnect(); } catch {}
    try { await pool.end(); } catch {}
    stopProcess(gateway);
    stopProcess(worker);
    setTimeout(() => process.exit(0), 500);
  }
});

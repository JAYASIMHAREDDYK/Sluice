import pg from 'pg';
import { Kafka } from 'kafkajs';

const PG_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/sluice';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = process.env.TOPIC || 'transactions';
const DLQ_TOPIC = process.env.DLQ_TOPIC || 'transactions.dlq';
const GROUP_ID = process.env.GROUP_ID || 'sluice-worker-group';

const pool = new pg.Pool({ connectionString: PG_URL });
pool.on('error', (err) => console.error('database pool error:', err.message));
const kafka = new Kafka({ clientId: 'worker', brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: GROUP_ID });
const producer = kafka.producer();

const attemptCounts = new Map();

export async function saveTransactions(pool, transactions) {
  if (transactions.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // sort distinct accounts by account_id to prevent deadlocks across concurrent worker batches
    const accounts = Array.from(
      new Map(transactions.map(t => [`${t.tenant_id}:${t.account_id}`, { tenant_id: t.tenant_id, account_id: t.account_id }])).values()
    ).sort((a, b) => a.account_id.localeCompare(b.account_id));

    for (const acc of accounts) {
      await client.query(
        'INSERT INTO accounts (tenant_id, account_id, balance) VALUES ($1, $2, 0) ON CONFLICT (tenant_id, account_id) DO NOTHING',
        [acc.tenant_id, acc.account_id]
      );
      await client.query(
        'SELECT balance FROM accounts WHERE tenant_id = $1 AND account_id = $2 FOR UPDATE',
        [acc.tenant_id, acc.account_id]
      );
    }

    for (const t of transactions) {
      const res = await client.query(
        'INSERT INTO idempotency_keys (tenant_id, idempotency_key) VALUES ($1, $2) ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING idempotency_key',
        [t.tenant_id, t.idempotency_key]
      );

      // if row was inserted, this is the first time we see this key so apply balance update
      if (res.rowCount > 0) {
        await client.query(
          'INSERT INTO transactions (id, tenant_id, account_id, amount, idempotency_key, created_at) VALUES ($1, $2, $3, $4, $5, $6)',
          [t.id, t.tenant_id, t.account_id, t.amount, t.idempotency_key, t.created_at || new Date().toISOString()]
        );
        await client.query(
          'UPDATE accounts SET balance = balance + $1 WHERE tenant_id = $2 AND account_id = $3',
          [t.amount, t.tenant_id, t.account_id]
        );
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export async function sendToDeadLetters(producer, dlqTopic, message, errorText) {
  const key = `${message.tenant_id}:${message.account_id}`;
  await producer.send({
    topic: dlqTopic,
    acks: -1,
    messages: [
      {
        key,
        value: JSON.stringify({
          ...message,
          error: errorText,
          failed_at: new Date().toISOString()
        })
      }
    ]
  });
}

const FROM_BEGINNING = process.env.FROM_BEGINNING === 'true';

async function read() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: FROM_BEGINNING });
  console.log(`worker listening to topic ${TOPIC}`);

  await consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      const items = [];
      for (const msg of batch.messages) {
        try {
          items.push({
            raw: msg,
            data: JSON.parse(msg.value.toString())
          });
        } catch (err) {
          console.error('unparseable message skipped:', err.message);
          resolveOffset(msg.offset);
        }
      }

      if (items.length === 0) {
        await commitOffsetsIfNecessary();
        return;
      }

      try {
        await saveTransactions(pool, items.map(item => item.data));
        for (const item of items) {
          attemptCounts.delete(item.raw.offset);
          resolveOffset(item.raw.offset);
        }
      } catch (err) {
        console.error('batch processing failed:', err.message);
        // process items one by one on batch failure to isolate poisoned messages
        for (const item of items) {
          const attempts = (attemptCounts.get(item.raw.offset) || 0) + 1;
          attemptCounts.set(item.raw.offset, attempts);

          try {
            await saveTransactions(pool, [item.data]);
            attemptCounts.delete(item.raw.offset);
            resolveOffset(item.raw.offset);
          } catch (itemErr) {
            if (attempts >= 5) {
              // dead letter after 5 attempts so bad payload does not block healthy partitions forever
              await sendToDeadLetters(producer, DLQ_TOPIC, item.data, itemErr.message);
              attemptCounts.delete(item.raw.offset);
              resolveOffset(item.raw.offset);
            } else {
              throw itemErr;
            }
          }
        }
      }

      await commitOffsetsIfNecessary();
      await heartbeat();
    }
  });
}

read().catch(err => {
  console.error('worker crashed:', err.message);
  process.exit(1);
});

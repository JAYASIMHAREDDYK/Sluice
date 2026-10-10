const DEFAULT_TTL = Number(process.env.IDEMPOTENCY_TTL_SECONDS || 86400);
const IN_FLIGHT_TTL = Number(process.env.IN_FLIGHT_TTL_SECONDS || 30);

const script = `
local key = KEYS[1]
local sha = ARGV[1]
local in_flight_ttl = tonumber(ARGV[2])
local initial_response = ARGV[3]

local exists = redis.call('EXISTS', key)
if exists == 0 then
  -- reserve key with short ttl so crash before kafka publish unblocks retries
  redis.call('HSET', key, 'sha', sha, 'status', 'queued', 'response', initial_response)
  redis.call('EXPIRE', key, in_flight_ttl)
  return { 0, '' }
end

local stored_sha = redis.call('HGET', key, 'sha')
local stored_response = redis.call('HGET', key, 'response')

if stored_sha == sha then
  return { 1, stored_response or initial_response }
else
  return { 2, '' }
end
`;

export async function alreadySeen(redis, tenantId, idempotencyKey, bodySha, initialResponse = {}, inFlightTtl = IN_FLIGHT_TTL) {
  if (!redis.checkSeen) {
    redis.defineCommand('checkSeen', {
      numberOfKeys: 1,
      lua: script
    });
  }
  const key = `seen:${tenantId}:${idempotencyKey}`;
  const responsePayload = JSON.stringify(initialResponse);
  const [code, stored] = await redis.checkSeen(key, bodySha, inFlightTtl, responsePayload);

  if (code === 0) {
    return { seen: false };
  }
  if (code === 1) {
    let parsed = initialResponse;
    try {
      parsed = JSON.parse(stored);
    } catch {}
    return { seen: true, match: true, response: parsed };
  }
  return { seen: true, match: false };
}

export async function markDone(redis, tenantId, idempotencyKey, response, ttl = DEFAULT_TTL) {
  const key = `seen:${tenantId}:${idempotencyKey}`;
  await redis.hset(key, 'response', JSON.stringify(response), 'status', 'done');
  await redis.expire(key, ttl);
}

export async function forgetKey(redis, tenantId, idempotencyKey) {
  const key = `seen:${tenantId}:${idempotencyKey}`;
  await redis.del(key);
}

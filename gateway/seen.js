const DEFAULT_TTL = Number(process.env.IDEMPOTENCY_TTL_SECONDS || 86400);

const script = `
local key = KEYS[1]
local sha = ARGV[1]
local ttl = tonumber(ARGV[2])
local initial_response = ARGV[3]

local exists = redis.call('EXISTS', key)
if exists == 0 then
  -- reserve key atomically to prevent concurrent requests from publishing duplicate messages
  redis.call('HSET', key, 'sha', sha, 'response', initial_response)
  redis.call('EXPIRE', key, ttl)
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

export async function alreadySeen(redis, tenantId, idempotencyKey, bodySha, initialResponse = {}, ttl = DEFAULT_TTL) {
  if (!redis.checkSeen) {
    redis.defineCommand('checkSeen', {
      numberOfKeys: 1,
      lua: script
    });
  }
  const key = `seen:${tenantId}:${idempotencyKey}`;
  const responsePayload = JSON.stringify(initialResponse);
  const [code, stored] = await redis.checkSeen(key, bodySha, ttl, responsePayload);

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
  await redis.hset(key, 'response', JSON.stringify(response));
  await redis.expire(key, ttl);
}

export async function forgetKey(redis, tenantId, idempotencyKey) {
  const key = `seen:${tenantId}:${idempotencyKey}`;
  await redis.del(key);
}

export const script = `
local time = redis.call('TIME')
local now = tonumber(time[1]) + (tonumber(time[2]) / 1000000)
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])

if not tokens then
  tokens = capacity
  last = now
else
  -- redis time is used instead of client clock so multi-node skew cannot corrupt bucket state
  local delta = math.max(0, now - last)
  tokens = math.min(capacity, tokens + delta * rate)
  last = now
end

if tokens >= 1 then
  tokens = tokens - 1
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last', last)
  redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 60)
  return { 1, 0 }
else
  local missing = 1 - tokens
  local retry_after = math.ceil(missing / rate)
  if retry_after < 1 then
    retry_after = 1
  end
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last', last)
  redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 60)
  return { 0, retry_after }
end
`;

export async function allowRequest(redis, tenantId, capacity = 100, rate = 50) {
  if (!redis.tokenBucket) {
    redis.defineCommand('tokenBucket', {
      numberOfKeys: 1,
      lua: script
    });
  }
  const key = `limit:${tenantId}`;
  const [allowed, retryAfter] = await redis.tokenBucket(key, capacity, rate);
  return {
    allowed: allowed === 1,
    retryAfter: Number(retryAfter)
  };
}

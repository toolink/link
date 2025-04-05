-- Lua script for atomic rate limiting (token bucket) in Redis
-- KEYS[1]: The unique key for the rate limit (e.g., "ratelimit:user:123:path:/api/data")
-- ARGV[1]: max_tokens (float, e.g., rate) -- The maximum number of tokens the bucket can hold (burst capacity).
-- ARGV[2]: tokens_per_second (float, e.g., rate / per) -- How many tokens are refilled per second.
-- ARGV[3]: current_timestamp (float, e.g., time.Now().UnixNano() / 1e9) -- Current time as Unix timestamp with fractional seconds.
-- ARGV[4]: tokens_to_consume (float, typically 1.0) -- How many tokens this request costs.

local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local tokens_per_second = tonumber(ARGV[2])
local current_timestamp = tonumber(ARGV[3])
local tokens_to_consume = tonumber(ARGV[4])

-- Fetch the current state [allowance, last_check_timestamp] using HGETALL
local state = redis.call('HGETALL', key)

local last_tokens -- initialize as nil
local last_check_timestamp

if #state == 0 then
  -- Key doesn't exist, first request
  last_tokens = max_tokens
  last_check_timestamp = current_timestamp
else
  -- Key exists, parse state (HGETALL returns ["field1", "value1", "field2", "value2", ...])
  for i = 1, #state, 2 do
    if state[i] == 'allowance' then
      last_tokens = tonumber(state[i+1])
    elseif state[i] == 'last_check' then
      last_check_timestamp = tonumber(state[i+1])
    end
  end
  -- handle potential case where fields might be missing if state gets corrupted somehow, though unlikely
  if last_tokens == nil or last_check_timestamp == nil then
     last_tokens = max_tokens
     last_check_timestamp = current_timestamp
     -- maybe log a warning server-side if possible/needed
  end
end


-- Calculate elapsed time and tokens to add
local time_passed = math.max(0, current_timestamp - last_check_timestamp)
local tokens_to_add = time_passed * tokens_per_second

-- Calculate new token count, clamped to max_tokens
local current_tokens = math.min(max_tokens, last_tokens + tokens_to_add)

-- Check if enough tokens are available
local allowed = current_tokens >= tokens_to_consume

local new_tokens
if allowed then
  -- Consume tokens
  new_tokens = current_tokens - tokens_to_consume
else
  -- Not allowed, tokens remain as they were after refill calculation
  new_tokens = current_tokens
end

-- Update the state in Redis using HMSET
redis.call('HMSET', key, 'allowance', new_tokens, 'last_check', current_timestamp)

-- Set an expiration time on the key to automatically clean up old entries
-- Use a reasonable expiry, e.g., twice the 'per' duration or longer,
-- Needs careful consideration based on expected traffic patterns.
-- Example: Expire after roughly 2 full refill cycles + some buffer (e.g., 60s)
-- We need 'per' value which is rate / tokens_per_second
local per_duration
if tokens_per_second > 0 then
    per_duration = max_tokens / tokens_per_second
else
    per_duration = 86400 -- default to a day if refill rate is zero? or handle error?
end
local expire_after_seconds = math.ceil(per_duration * 2 + 60)
redis.call('EXPIRE', key, expire_after_seconds)


-- Return 1 if allowed, 0 if denied
if allowed then
  return 1
else
  return 0
end
-- Unified Latency-based AIMD Slow-Start (success/failure by flag)
-- Implements slow-start multiply by n, and additive increase by m after hitting max.
-- Also applies multiplicative decrease (md) on consecutive high-latency failures.
--
-- KEYS[1]: rate:config:ORG_ID (hash: limit_qps)
-- KEYS[2]: rate:viol:ORG_ID (string counter for consecutive high-latency)
-- ARGV[1]: success flag (1=success, 0=failure)
-- ARGV[2]: max_limit
-- ARGV[3]: min_limit
-- ARGV[4]: n (multiply factor for slow-start, default 2)
-- ARGV[5]: m (additive increment after slow-start, default 1)
-- ARGV[6]: latency_ms
-- ARGV[7]: latency_threshold_ms (default 500)
-- ARGV[8]: md (multiplicative decrease factor, default 0.5)
-- ARGV[9]: n_threshold (consecutive high-latency to trigger, default 3)
-- ARGV[10]: counter_ttl_sec (default 60)

local key = KEYS[1]
local counter_key = KEYS[2]

local success = tonumber(ARGV[1]) or 1
local max_limit = tonumber(ARGV[2]) or 100000
local min_limit = tonumber(ARGV[3]) or 1
local n = tonumber(ARGV[4]) or 2
local m = tonumber(ARGV[5]) or 1
local latency = tonumber(ARGV[6]) or 0
local latency_threshold = tonumber(ARGV[7]) or 500
local md = tonumber(ARGV[8]) or 0.5
local n_threshold = tonumber(ARGV[9]) or 3
local counter_ttl = tonumber(ARGV[10]) or 60

-- normalize parameters
if n < 1 then n = 1 end
if m < 1 then m = 1 end
if md <= 0 then md = 0.5 end
if md >= 1 then md = 0.9 end

-- Detect legacy key type and capture legacy value if string
local keyType = redis.call('TYPE', key)
local typeStr = keyType
if type(keyType) == 'table' then
  typeStr = keyType['ok']
end
local legacy_limit = nil
if typeStr == 'string' then
  legacy_limit = tonumber(redis.call('GET', key))
  redis.call('DEL', key)
end

-- Read current limit from hash
local res = redis.call('HMGET', key, 'limit_qps')
local limit_qps = tonumber(res[1])
if limit_qps == nil and legacy_limit ~= nil then
  limit_qps = legacy_limit
end
if limit_qps == nil then limit_qps = 1000 end

-- Violation counter logic (for latency-triggered failures)
local triggered = false
if latency >= latency_threshold then
  local c = tonumber(redis.call('INCR', counter_key)) or 1
  if counter_ttl > 0 then redis.call('EXPIRE', counter_key, counter_ttl) end
  if c >= n_threshold then
    triggered = true
    redis.call('DEL', counter_key)
  end
else
  if success >= 1 then
    redis.call('DEL', counter_key)
  end
end

local next_limit = limit_qps

if success >= 1 and not triggered then
  -- Success path: slow-start multiply by n up to max; otherwise add m up to max
  local multiplied = limit_qps * n
  if multiplied <= max_limit then
    next_limit = multiplied
  else
    local added = limit_qps + m
    if added > max_limit then added = max_limit end
    next_limit = added
  end
else
  -- Failure path (explicit failure flag or triggered by latency)
  local decreased = math.floor(limit_qps * md)
  if decreased < min_limit then decreased = min_limit end
  next_limit = decreased
end

next_limit = math.floor(next_limit)

redis.call('HSET', key, 'limit_qps', next_limit)
return next_limit
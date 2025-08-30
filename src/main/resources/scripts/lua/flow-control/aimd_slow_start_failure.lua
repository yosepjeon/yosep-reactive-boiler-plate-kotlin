-- KEYS[1] = config key
-- ARGV[1] = min limit
local key = KEYS[1]
local min = tonumber(ARGV[1])
local current = tonumber(redis.call("GET", key)) or 1000
local next = math.max(math.floor(current * 0.5), min)
redis.call("SET", key, next)
return next
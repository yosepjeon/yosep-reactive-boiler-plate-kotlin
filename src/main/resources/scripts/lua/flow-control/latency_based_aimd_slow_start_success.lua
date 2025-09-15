-- KEYS[1] = config key
-- ARGV[1] = max limit

local key = KEYS[1]
local max = tonumber(ARGV[1])
local current = tonumber(redis.call("GET", key)) or 1000

local next = 0
local double = current * 2

if double <= max then
  next = double
else
  next = math.min(current + 1, max)
end

redis.call("SET", key, next)
return next
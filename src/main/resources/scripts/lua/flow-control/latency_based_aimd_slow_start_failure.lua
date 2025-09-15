-- AIMD Slow-Start: High-latency triggered multiplicative decrease
-- KEYS[1] = limit key
-- ARGV[1] = min_limit
-- ARGV[2] = latency_ms
-- ARGV[3] = latency_exceed_count (옵션, 기본 10)
-- ARGV[4] = threshold_ms (옵션, 기본 500)

local key          = KEYS[1]
local min_limit    = tonumber(ARGV[1]) or 1
local latency_ms   = tonumber(ARGV[2]) or 0
local latency_exceed_count = tonumber(redis.call("GET", key .. ":latency_exceed_count")) or 0
local latency_exceed_count_limit = tonumber(ARGV[3]) or 10
local threshold_ms = tonumber(ARGV[3]) or 500

-- 현재 한계값(없으면 1000으로 초기화)
local current = tonumber(redis.call("GET", key)) or 1000

-- 임계값 이상일 때만 감속
if latency_ms >= threshold_ms and latency_exceed_count > latency_exceed_count_limit  then
    local next = math.max(math.floor(current * 0.5), min_limit)
    redis.call("SET", key, next)
    redis.call("SET", key .. ":latency_exceed_count", latency_exceed_count + 1)
    return next
end

-- 변화 없음: 현재값 반환
return current

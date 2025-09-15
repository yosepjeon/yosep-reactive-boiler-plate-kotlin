-- KEYS[1]: rate:config:ORG_ID (Hash)
-- ARGV[1]: latency_ms (없으면 latency_threshold * 1.2 사용)
-- ARGV[2]: now_ms
-- ARGV[3]: tau_seconds             (시간 상수 τ, 기본 5)
-- ARGV[4]: max_limit
-- ARGV[5]: min_limit
-- ARGV[6]: latency_threshold_ms    (없으면 500)

local key = KEYS[1]
local raw_latency = ARGV[1]
local now_ms = tonumber(ARGV[2]) or 0
local tau_sec = tonumber(ARGV[3]) or 5.0
local max_limit = tonumber(ARGV[4]) or 100000
local min_limit = tonumber(ARGV[5]) or 30
local latency_threshold = tonumber(ARGV[6]) or 500.0

local success = 0.0
local latency = tonumber(raw_latency)
if latency == nil then latency = latency_threshold * 1.2 end

-- 기존 상태 + 이전 타임스탬프
local res = redis.call('HMGET', key, 'ewma_success', 'ewma_latency', 'limit_qps', 'ts_ms')
local ewma_success = tonumber(res[1])
local ewma_latency = tonumber(res[2])
local limit_qps    = tonumber(res[3])
local prev_ts      = tonumber(res[4])

-- cold-start
if limit_qps == nil then limit_qps = 1000 end
if ewma_success == nil then ewma_success = success end
if ewma_latency == nil then ewma_latency = latency end

-- 시간기반 EWMA: alpha = 1 - exp(-Δt/τ)
local alpha = 1.0
if prev_ts ~= nil and now_ms > prev_ts then
    local dt_sec = (now_ms - prev_ts) / 1000.0
    if dt_sec < 0 then dt_sec = 0 end
    if tau_sec <= 0 then tau_sec = 5.0 end
    alpha = 1.0 - math.exp(-dt_sec / tau_sec)
    if alpha < 0 then alpha = 0 end
    if alpha > 1 then alpha = 1 end
end

-- EWMA 갱신
ewma_success = alpha * success + (1 - alpha) * ewma_success
ewma_latency = alpha * latency + (1 - alpha) * ewma_latency

-- AIMD
if (ewma_success < 0.95) or (ewma_latency > latency_threshold) then
    limit_qps = math.max(math.floor(limit_qps * 0.7), min_limit)
else
    limit_qps = math.min(limit_qps + 100, max_limit)
end

limit_qps = math.floor(limit_qps)

-- 저장 (+ 현재 타임스탬프)
redis.call('HSET', key,
        'ewma_success', ewma_success,
        'ewma_latency', ewma_latency,
        'limit_qps',    limit_qps,
        'ts_ms',        now_ms
)
return limit_qps

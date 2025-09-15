---- KEYS[1]: rate:config:ORG_ID
---- ARGV[1]: success (1 or 0)
---- ARGV[2]: latency
---- ARGV[3]: alpha
---- ARGV[4]: max_limit
---- ARGV[5]: min_limit
---- ARGV[6]: latency_threshold ← ★ 새로 추가된 동적 파라미터
--
--local key = KEYS[1]
--local success = tonumber(ARGV[1])
--local latency = tonumber(ARGV[2])
--local alpha = tonumber(ARGV[3])
--local max_limit = tonumber(ARGV[4])
--local min_limit = tonumber(ARGV[5])
--local latency_threshold = tonumber(ARGV[6]) or 500  -- fallback
--
---- 기본값들
--local ewma_success = 1.0
--local ewma_latency = 0.0
--local limit_qps = 1000
--
---- 기존 값 읽기
--local data = redis.call("HGETALL", key)
--for i=1,#data,2 do
--    if data[i] == "ewma_success" then ewma_success = tonumber(data[i+1]) end
--    if data[i] == "ewma_latency" then ewma_latency = tonumber(data[i+1]) end
--    if data[i] == "limit_qps" then limit_qps = tonumber(data[i+1]) end
--end
--
---- EWMA 업데이트
--ewma_success = alpha * success + (1 - alpha) * ewma_success
--ewma_latency = alpha * latency + (1 - alpha) * ewma_latency
--
---- AIMD 조절
--if ewma_success < 0.95 or ewma_latency > latency_threshold then
--    limit_qps = math.max(math.floor(limit_qps * 0.7), min_limit)
--else
--    limit_qps = math.min(limit_qps + 100, max_limit)
--end
--
---- 저장
--redis.call("HSET", key, "ewma_success", ewma_success)
--redis.call("HSET", key, "ewma_latency", ewma_latency)
--redis.call("HSET", key, "limit_qps", limit_qps)
--
--return limit_qps

-- KEYS[1]: rate:config:ORG_ID
-- ARGV[1]: success (1 or 0)
-- ARGV[2]: latency
-- ARGV[3]: alpha (0~1 권장)
-- ARGV[4]: max_limit
-- ARGV[5]: min_limit
-- ARGV[6]: latency_threshold (없으면 500)

-- KEYS[1]: rate:config:ORG_ID (Hash)
-- ARGV[1]: latency_ms (없으면 latency_threshold*1.2 사용)
-- ARGV[2]: alpha (0~1 권장)
-- ARGV[3]: max_limit
-- ARGV[4]: min_limit
-- ARGV[5]: latency_threshold_ms (없으면 500)

local key = KEYS[1]
local raw_latency = ARGV[1]
local alpha = tonumber(ARGV[2])
local max_limit = tonumber(ARGV[3])
local min_limit = tonumber(ARGV[4])
local latency_threshold = tonumber(ARGV[5]) or 500.0
local success = 0.0

-- latency 파싱(없으면 보수적으로 threshold*1.2)
local latency = tonumber(raw_latency)
if latency == nil then latency = latency_threshold * 1.2 end

-- alpha 보정
if alpha == nil or alpha ~= alpha then alpha = 0.2 end
if alpha < 0 then alpha = 0 end
if alpha > 1 then alpha = 1 end

-- 기존 값 읽기
local res = redis.call('HMGET', key, 'ewma_success', 'ewma_latency', 'limit_qps')
local ewma_success = tonumber(res[1])
local ewma_latency = tonumber(res[2])
local limit_qps = tonumber(res[3])

-- cold-start
if limit_qps == nil then limit_qps = 1000 end
if ewma_success == nil then ewma_success = success end
if ewma_latency == nil then ewma_latency = latency end

-- EWMA 업데이트
ewma_success = alpha * success + (1 - alpha) * ewma_success
ewma_latency = alpha * latency + (1 - alpha) * ewma_latency

-- AIMD
if (ewma_success < 0.95) or (ewma_latency > latency_threshold) then
    limit_qps = math.max(math.floor(limit_qps * 0.7), min_limit)
else
    limit_qps = math.min(limit_qps + 100, max_limit)
end

limit_qps = math.floor(limit_qps)

-- 저장
redis.call('HSET', key,
        'ewma_success', ewma_success,
        'ewma_latency', ewma_latency,
        'limit_qps', limit_qps
)

-- 옵션: TTL
-- redis.call('EXPIRE', key, 86400)

return limit_qps


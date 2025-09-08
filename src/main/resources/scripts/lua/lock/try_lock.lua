-- KEYS[1] = cb:{name}:lock
-- ARGV[1] = ttl_ms

local lockKey = KEYS[1]
local ttl = tonumber(ARGV[1]) or 5000

-- Redis 서버 시간 사용
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local expire_at = now_ms + ttl

-- 토큰 = expire_at (간단 버전). 필요시 증가시퀀스/UUID로 교체 가능.
local ok = redis.call('SET', lockKey, tostring(expire_at), 'NX', 'PX', ttl)
if ok then
    return cjson.encode({ acquired=true, token=tostring(expire_at), now=now_ms })
end

local current = tonumber(redis.call('GET', lockKey))
if current ~= nil and now_ms > current then
    -- 만료 선점
    redis.call('SET', lockKey, tostring(expire_at), 'PX', ttl)
    return cjson.encode({ acquired=true, token=tostring(expire_at), now=now_ms, preempted=true })
end

return cjson.encode({ acquired=false, now=now_ms })

-- KEYS[1] = cb:{name}:state
-- ARGV[1] = fromState
-- ARGV[2] = toState
--
-- 내부에서 version을 +1 하여 원자적으로 전환한다.
-- state hash fields: state, version, lastFrom, lastTo

local key = KEYS[1]
local fromState = ARGV[1]
local toState = ARGV[2]

-- 현재 상태/버전 조회
local data = redis.call('HGETALL', key)
local currentState, currentVersion = nil, 0
for i = 1, #data, 2 do
    local f = data[i]
    if f == 'state' then currentState = data[i+1] end
    if f == 'version' then currentVersion = tonumber(data[i+1]) end
end
if currentState == false or currentState == nil then
    currentState = 'CLOSED'
    currentVersion = 0
end

-- 전이 유효성
local valid = (fromState == 'CLOSED' and toState == 'OPEN')
        or (fromState == 'OPEN' and toState == 'HALF_OPEN')
        or (fromState == 'HALF_OPEN' and (toState == 'CLOSED' or toState == 'OPEN'))

if not valid then
    return cjson.encode({ result='INVALID', currentState=currentState, currentVersion=currentVersion })
end

if currentState ~= fromState then
    return cjson.encode({ result='STALE_STATE', currentState=currentState, currentVersion=currentVersion })
end

-- 전이 수행 (원자적)
local newVersion = currentVersion + 1
redis.call('HSET', key,
        'state', toState,
        'version', newVersion,
        'lastFrom', fromState,
        'lastTo', toState
)

-- Pub/Sub 알림(더 풍부한 페이로드)
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local payload = cjson.encode({
    nameKey = key,  -- 예: "cb:{payments}:state"
    from = fromState,
    to = toState,
    version = newVersion,
    ts = now_ms
})
redis.call('PUBLISH', 'cb:pubsub', payload)

return cjson.encode({ result='OK', currentState=toState, currentVersion=newVersion })

-- KEYS[1]: counter key prefix (ex: rate:limit:org:{nh_card})  -- Cluster면 해시태그 권장
-- ARGV[1]: window size (ms)
-- ARGV[2]: max allowed count per window

local key = KEYS[1]
local windowSize = tonumber(ARGV[1])
local maxCount = tonumber(ARGV[2])

-- 1) Redis server time -> milliseconds
local t = redis.call("TIME")                 -- { sec, usec }
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)

-- 2) 윈도 계산
local currentWindow  = math.floor(now / windowSize)
local previousWindow = currentWindow - 1

local currentKey  = key .. ":" .. currentWindow
local previousKey = key .. ":" .. previousWindow

-- 3) 카운트 조회
local currentCount  = tonumber(redis.call("GET", currentKey)) or 0
local previousCount = tonumber(redis.call("GET", previousKey)) or 0

-- 4) 가중치 + 추정치
local elapsedInCurrent = now - (currentWindow * windowSize)  -- ms
local weight = elapsedInCurrent / windowSize                 -- 0..1

local estimatedCount = previousCount * (1 - weight) + currentCount

-- 5) 허용/차단 + TTL
if estimatedCount >= maxCount then
    return 0
else
    redis.call("INCR", currentKey)
    redis.call("PEXPIRE", currentKey, windowSize * 2)  -- 필요시 PEXPIREAT로 경계고정 만료 가능
    return 1
end

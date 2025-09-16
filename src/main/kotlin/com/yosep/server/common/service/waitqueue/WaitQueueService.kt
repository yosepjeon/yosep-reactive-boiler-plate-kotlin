package com.yosep.server.common.service.waitqueue

import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.redisson.api.RScript
import org.redisson.api.RedissonReactiveClient
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant

@Service
class WaitQueueService(
    private val redissonReactiveClient: RedissonReactiveClient
) {
    companion object {
        const val WAIT_QUEUE_KEY = "queue:wait"
        const val ENTER_QUEUE_KEY = "queue:enter"
        const val QUEUE_CONFIG_KEY = "queue:config"
        const val QUEUE_METRICS_KEY = "queue:metrics"
        const val USER_TOKEN_PREFIX = "queue:token:"
        const val QUEUE_LOCK_KEY = "queue:transition:lock"

        // Default configurations
        const val DEFAULT_TOKEN_TTL_SECONDS = 3600L // 1 hour
        const val DEFAULT_TRANSITION_INTERVAL_MS = 1000L // 1 second
        const val DEFAULT_BATCH_SIZE = 100
    }

    /**
     * 사용자를 대기열에 추가
     * @param userId 사용자 ID
     * @param priority 우선순위 (낮을수록 우선)
     * @return 대기열 위치
     */
    suspend fun joinWaitQueue(userId: String, priority: Double = System.currentTimeMillis().toDouble()): Long {
        val script = """
            -- 이미 입장열에 있는지 확인
            local inEnterQueue = redis.call('ZSCORE', KEYS[2], ARGV[1])
            if inEnterQueue then
                return -1 -- 이미 입장열에 있음
            end

            -- 이미 대기열에 있는지 확인
            local existingScore = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if existingScore then
                -- 이미 대기열에 있으면 현재 위치 반환
                return redis.call('ZRANK', KEYS[1], ARGV[1]) + 1
            end

            -- 대기열에 추가
            redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])

            -- 토큰 생성 및 TTL 설정
            redis.call('SETEX', KEYS[3], ARGV[3], ARGV[4])

            -- 메트릭 업데이트
            redis.call('HINCRBY', KEYS[4], 'total_joined', 1)
            redis.call('HSET', KEYS[4], 'last_join_time', ARGV[4])

            -- 현재 위치 반환
            return redis.call('ZRANK', KEYS[1], ARGV[1]) + 1
        """.trimIndent()

        val keys = listOf(
            WAIT_QUEUE_KEY,
            ENTER_QUEUE_KEY,
            "$USER_TOKEN_PREFIX$userId",
            QUEUE_METRICS_KEY
        )
        val values = listOf(
            userId,
            priority.toString(),
            DEFAULT_TOKEN_TTL_SECONDS.toString(),
            System.currentTimeMillis().toString()
        )

        return redissonReactiveClient.script
            .eval<Long>(RScript.Mode.READ_WRITE, script, RScript.ReturnType.INTEGER, keys, *values.toTypedArray())
            .awaitFirst()
    }

    /**
     * 사용자의 현재 대기 상태 조회
     */
    suspend fun getQueueStatus(userId: String): QueueStatus {
        val script = """
            -- 입장열 확인
            local inEnterQueue = redis.call('ZSCORE', KEYS[2], ARGV[1])
            if inEnterQueue then
                return cjson.encode({
                    status = 'ENTERED',
                    position = 0,
                    estimatedWaitTime = 0,
                    queueSize = redis.call('ZCARD', KEYS[2])
                })
            end

            -- 대기열 확인
            local inWaitQueue = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if inWaitQueue then
                local position = redis.call('ZRANK', KEYS[1], ARGV[1]) + 1
                local queueSize = redis.call('ZCARD', KEYS[1])
                local avgProcessingTime = redis.call('HGET', KEYS[3], 'avg_processing_time') or '60'
                local estimatedWait = position * tonumber(avgProcessingTime)

                return cjson.encode({
                    status = 'WAITING',
                    position = position,
                    estimatedWaitTime = estimatedWait,
                    queueSize = queueSize
                })
            end

            -- 대기열에 없음
            return cjson.encode({
                status = 'NOT_IN_QUEUE',
                position = -1,
                estimatedWaitTime = -1,
                queueSize = redis.call('ZCARD', KEYS[1])
            })
        """.trimIndent()

        val keys = listOf(WAIT_QUEUE_KEY, ENTER_QUEUE_KEY, QUEUE_METRICS_KEY)
        val values = listOf(userId)

        val result = redissonReactiveClient.script
            .eval<String>(RScript.Mode.READ_ONLY, script, RScript.ReturnType.VALUE, keys, *values.toTypedArray())
            .awaitFirst()

        return parseQueueStatus(result)
    }

    /**
     * 대기열에서 입장열로 사용자 이동 (배압 기반)
     * @param batchSize 한 번에 이동할 사용자 수
     * @return 이동된 사용자 ID 목록
     */
    suspend fun transitionToEnterQueue(batchSize: Int): List<String> {
        val script = """
            -- 분산 락 획득 시도
            local lockKey = KEYS[4]
            local lockValue = ARGV[2]
            local lockTTL = 5

            local acquired = redis.call('SET', lockKey, lockValue, 'NX', 'EX', lockTTL)
            if not acquired then
                return {} -- 다른 노드가 이미 처리 중
            end

            -- 대기열에서 상위 N명 가져오기
            local waitingUsers = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)

            if #waitingUsers == 0 then
                redis.call('DEL', lockKey)
                return {}
            end

            local transitioned = {}
            local currentTime = ARGV[3]

            for i, userId in ipairs(waitingUsers) do
                -- 토큰 유효성 확인
                local tokenKey = KEYS[5] .. userId
                local tokenValid = redis.call('EXISTS', tokenKey)

                if tokenValid == 1 then
                    -- 대기열에서 제거
                    redis.call('ZREM', KEYS[1], userId)
                    -- 입장열에 추가
                    redis.call('ZADD', KEYS[2], currentTime, userId)
                    -- 토큰 갱신
                    redis.call('EXPIRE', tokenKey, 3600)
                    table.insert(transitioned, userId)
                else
                    -- 토큰이 없으면 대기열에서도 제거 (이탈 처리)
                    redis.call('ZREM', KEYS[1], userId)
                end
            end

            -- 메트릭 업데이트
            if #transitioned > 0 then
                redis.call('HINCRBY', KEYS[3], 'total_entered', #transitioned)
                redis.call('HSET', KEYS[3], 'last_transition_time', currentTime)
                redis.call('HSET', KEYS[3], 'last_transition_count', #transitioned)
            end

            -- 락 해제
            redis.call('DEL', lockKey)

            return transitioned
        """.trimIndent()

        val keys = listOf(
            WAIT_QUEUE_KEY,
            ENTER_QUEUE_KEY,
            QUEUE_METRICS_KEY,
            QUEUE_LOCK_KEY,
            USER_TOKEN_PREFIX
        )
        val values = listOf(
            batchSize.toString(),
            "${System.nanoTime()}-${Thread.currentThread().id}",
            System.currentTimeMillis().toString()
        )

        val result = redissonReactiveClient.script
            .eval<List<String>>(RScript.Mode.READ_WRITE, script, RScript.ReturnType.MULTI, keys, *values.toTypedArray())
            .awaitFirst()

        return result
    }

    /**
     * 사용자 이탈 처리 (대기열 초기화)
     */
    suspend fun handleUserExit(userId: String) {
        val script = """
            -- 토큰 삭제
            redis.call('DEL', KEYS[3])

            -- 대기열에서 제거
            local removedFromWait = redis.call('ZREM', KEYS[1], ARGV[1])

            -- 입장열에서 제거
            local removedFromEnter = redis.call('ZREM', KEYS[2], ARGV[1])

            -- 메트릭 업데이트
            if removedFromWait == 1 or removedFromEnter == 1 then
                redis.call('HINCRBY', KEYS[4], 'total_exited', 1)
                redis.call('HSET', KEYS[4], 'last_exit_time', ARGV[2])
            end

            return removedFromWait + removedFromEnter
        """.trimIndent()

        val keys = listOf(
            WAIT_QUEUE_KEY,
            ENTER_QUEUE_KEY,
            "$USER_TOKEN_PREFIX$userId",
            QUEUE_METRICS_KEY
        )
        val values = listOf(userId, System.currentTimeMillis().toString())

        redissonReactiveClient.script
            .eval<Long>(RScript.Mode.READ_WRITE, script, RScript.ReturnType.INTEGER, keys, *values.toTypedArray())
            .awaitFirstOrNull()
    }

    /**
     * 입장열 사용자 검증
     */
    suspend fun isUserInEnterQueue(userId: String): Boolean {
        val score = redissonReactiveClient.getScoredSortedSet<String>(ENTER_QUEUE_KEY)
            .getScore(userId)
            .awaitFirstOrNull()
        return score != null
    }

    /**
     * 대기열 메트릭 조회
     */
    suspend fun getQueueMetrics(): QueueMetrics {
        val waitQueueSet = redissonReactiveClient.getScoredSortedSet<String>(WAIT_QUEUE_KEY)
        val enterQueueSet = redissonReactiveClient.getScoredSortedSet<String>(ENTER_QUEUE_KEY)
        val metricsMap = redissonReactiveClient.getMap<String, String>(QUEUE_METRICS_KEY)

        val waitQueueSize = waitQueueSet.size().awaitFirstOrNull() ?: 0
        val enterQueueSize = enterQueueSet.size().awaitFirstOrNull() ?: 0
        val metrics = metricsMap.readAllMap().awaitFirstOrNull() ?: emptyMap()

        return QueueMetrics(
            waitQueueSize = waitQueueSize,
            enterQueueSize = enterQueueSize,
            totalJoined = metrics["total_joined"]?.toLongOrNull() ?: 0,
            totalEntered = metrics["total_entered"]?.toLongOrNull() ?: 0,
            totalExited = metrics["total_exited"]?.toLongOrNull() ?: 0,
            lastTransitionTime = metrics["last_transition_time"]?.toLongOrNull() ?: 0,
            lastTransitionCount = metrics["last_transition_count"]?.toIntOrNull() ?: 0,
            avgProcessingTime = metrics["avg_processing_time"]?.toDoubleOrNull() ?: 60.0
        )
    }

    /**
     * 입장열 정리 (오래된 사용자 제거)
     */
    suspend fun cleanupEnterQueue(maxAge: Duration): Int {
        val cutoffTime = Instant.now().minus(maxAge).toEpochMilli()
        val enterQueueSet = redissonReactiveClient.getScoredSortedSet<String>(ENTER_QUEUE_KEY)
        val removed = enterQueueSet.removeRangeByScore(
            0.0,
            true,
            cutoffTime.toDouble(),
            true
        ).awaitFirstOrNull() ?: 0
        return removed
    }

    private fun parseQueueStatus(json: String): QueueStatus {
        // Simple JSON parsing (consider using Jackson for production)
        val status = if (json.contains("ENTERED")) "ENTERED"
                     else if (json.contains("WAITING")) "WAITING"
                     else "NOT_IN_QUEUE"

        val position = Regex(""""position"\s*:\s*(\d+)""").find(json)?.groupValues?.get(1)?.toLong() ?: -1
        val estimatedWaitTime = Regex(""""estimatedWaitTime"\s*:\s*(\d+)""").find(json)?.groupValues?.get(1)?.toLong() ?: -1
        val queueSize = Regex(""""queueSize"\s*:\s*(\d+)""").find(json)?.groupValues?.get(1)?.toInt() ?: 0

        return QueueStatus(status, position, estimatedWaitTime, queueSize)
    }
}

data class QueueStatus(
    val status: String, // WAITING, ENTERED, NOT_IN_QUEUE
    val position: Long,
    val estimatedWaitTime: Long, // seconds
    val queueSize: Int
)

data class QueueMetrics(
    val waitQueueSize: Int,
    val enterQueueSize: Int,
    val totalJoined: Long,
    val totalEntered: Long,
    val totalExited: Long,
    val lastTransitionTime: Long,
    val lastTransitionCount: Int,
    val avgProcessingTime: Double
)
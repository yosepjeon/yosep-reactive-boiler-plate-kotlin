package com.yosep.server.common.component.ratelimit.local.atomic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLongArray

/**
 * 슬라이딩 윈도우 카운터 알고리즘 기반 Rate Limiter (Lock-free)
 *
 * 알고리즘:
 * 1. 시간을 작은 버킷(예: 100ms)으로 나눔
 * 2. 각 버킷에 요청 수를 기록
 * 3. 현재 시점 기준 과거 windowMs 내의 버킷들의 합계 계산
 * 4. 오래된 버킷은 자동으로 무효화됨
 *
 * 장점:
 * - 정확한 슬라이딩 윈도우 구현
 * - Lock-free로 높은 동시성 성능
 * - 메모리 효율적 (순환 버퍼 사용)
 * - 버스트 트래픽 정확히 제한
 *
 * 구현 특징:
 * - AtomicLongArray로 버킷별 카운터 관리
 * - CAS 연산으로 동시성 제어
 * - 시간 기반 자동 버킷 순환
 */
@Component
class LocalSlidingWindowCounterRateLimiterAtomic {
    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        // 버킷 크기 (밀리초). 작을수록 정확하지만 메모리 사용량 증가
        const val BUCKET_SIZE_MS = 100L
        // 최대 윈도우 크기 (60초 = 1분)
        const val MAX_WINDOW_MS = 60000L
        // 버킷 개수 (600개 = 60초 / 100ms)
        const val NUM_BUCKETS = (MAX_WINDOW_MS / BUCKET_SIZE_MS).toInt()
    }

    /**
     * @since: 2025-09-12
     * 슬라이딩 윈도우 상태
     * - buckets: 각 시간 버킷의 카운터 (순환 버퍼)
     * - lastUpdateTime: 마지막 업데이트 시간 (버킷 순환용)
     * @author : 전요셉
     */
    private class SlidingWindow {
        val buckets = AtomicLongArray(NUM_BUCKETS)
        @Volatile
        var lastUpdateTime = System.currentTimeMillis()

        /**
         * 현재 시간의 버킷 인덱스 계산
         * @param timeMs 기준 시간 (밀리초)
         * @author: 전요셉
         */
        private fun getBucketIndex(timeMs: Long): Int {
            return ((timeMs / BUCKET_SIZE_MS) % NUM_BUCKETS).toInt()
        }

        /**
         * @since: 2025-09-12
         * 오래된 버킷 정리 (순환)
         * @param currentTime 현재 시간 (밀리초)
         * * @author: 전요셉
         */
        fun cleanOldBuckets(currentTime: Long) {
            val elapsed = currentTime - lastUpdateTime
            if (elapsed >= BUCKET_SIZE_MS) {
                val bucketsToClean = minOf((elapsed / BUCKET_SIZE_MS).toInt(), NUM_BUCKETS)
                val startIdx = getBucketIndex(lastUpdateTime + BUCKET_SIZE_MS)

                for (i in 0 until bucketsToClean) {
                    val idx = (startIdx + i) % NUM_BUCKETS
                    buckets.set(idx, 0)
                }

                lastUpdateTime = currentTime - (currentTime % BUCKET_SIZE_MS)
            }
        }

        /**
         * @since: 2025-09-12
         * 요청 기록 (원자적 증가)
         * @author: 전요셉
         */
        fun recordRequest(currentTime: Long): Long {
            cleanOldBuckets(currentTime)
            val idx = getBucketIndex(currentTime)
            return buckets.incrementAndGet(idx)
        }

        /**
         * @since: 2025-09-12
         * 윈도우 내 총 요청 수 계산
         * @param currentTime 현재 시간 (밀리초)
         * @param windowMs 윈도우 크기 (밀리초)
         * @author: 전요셉
         */
        fun getCount(currentTime: Long, windowMs: Long): Long {
            cleanOldBuckets(currentTime)

            val windowBuckets = minOf((windowMs / BUCKET_SIZE_MS).toInt(), NUM_BUCKETS)
            val endIdx = getBucketIndex(currentTime)
            var total = 0L

            for (i in 0 until windowBuckets) {
                val idx = (endIdx - i + NUM_BUCKETS) % NUM_BUCKETS
                val bucketTime = currentTime - (i * BUCKET_SIZE_MS)

                // 버킷이 윈도우 내에 있는지 확인
                if (currentTime - bucketTime <= windowMs) {
                    total += buckets.get(idx)
                }
            }

            return total
        }

        /**
         * @since: 2025-09-12
         * 부분 윈도우 계산 (더 정확한 슬라이딩)
         * 현재 버킷과 윈도우 시작 버킷의 부분 카운트 포함
         * @param currentTime 현재 시간 (밀리초)
         * @param windowMs 윈도우 크기 (밀리초)
         * @author: 전요셉
         */
        fun getPreciseCount(currentTime: Long, windowMs: Long): Double {
            cleanOldBuckets(currentTime)

            val windowStart = currentTime - windowMs
            val endBucket = getBucketIndex(currentTime)

            var total = 0.0
            val windowBuckets = minOf((windowMs / BUCKET_SIZE_MS + 1).toInt(), NUM_BUCKETS)

            for (i in 0 until windowBuckets) {
                val idx = (endBucket - i + NUM_BUCKETS) % NUM_BUCKETS
                val bucketStartTime = (currentTime / BUCKET_SIZE_MS - i) * BUCKET_SIZE_MS
                val bucketEndTime = bucketStartTime + BUCKET_SIZE_MS

                if (bucketEndTime < windowStart) break

                val count = buckets.get(idx).toDouble()

                // 부분 버킷 처리
                if (bucketStartTime < windowStart) {
                    // 윈도우 시작 부분
                    val fraction = (bucketEndTime - windowStart) / BUCKET_SIZE_MS.toDouble()
                    total += count * fraction
                } else if (bucketEndTime > currentTime) {
                    // 현재 시간 부분 (일반적으로 발생하지 않음)
                    val fraction = (currentTime - bucketStartTime) / BUCKET_SIZE_MS.toDouble()
                    total += count * fraction
                } else {
                    // 완전한 버킷
                    total += count
                }
            }

            return total
        }

        /**
         * @since: 2025-09-12
         * 버킷 리셋
         * @author: 전요셉
         */
        fun reset() {
            for (i in 0 until NUM_BUCKETS) {
                buckets.set(i, 0)
            }
            lastUpdateTime = System.currentTimeMillis()
        }
    }

    // 각 키별 슬라이딩 윈도우
    private val windows = ConcurrentHashMap<String, SlidingWindow>()

    /**
     * @since: 2025-09-12
     * Rate limit 체크 및 획득
     * @param key 제한 키
     * @param maxCount 윈도우 내 최대 허용 요청 수
     * @param windowMs 윈도우 크기 (밀리초)
     * @param usePrecise 정확한 부분 윈도우 계산 사용 여부
     * @author: 전요셉
     */
    suspend fun tryAcquire(
        key: String,
        maxCount: Int,
        windowMs: Long,
        usePrecise: Boolean = false
    ): Boolean {
        require(windowMs in BUCKET_SIZE_MS..MAX_WINDOW_MS) {
            "Window size must be between $BUCKET_SIZE_MS and $MAX_WINDOW_MS ms"
        }

        val window = windows.computeIfAbsent(key) { SlidingWindow() }
        val currentTime = System.currentTimeMillis()

        // 현재 카운트 확인
        val currentCount = if (usePrecise) {
            window.getPreciseCount(currentTime, windowMs)
        } else {
            window.getCount(currentTime, windowMs).toDouble()
        }

        if (currentCount >= maxCount) {
            if (logger.isDebugEnabled) {
                logger.debug(
                    "[SlidingWindow] Rate limit exceeded for key: {}, count: {}/{}",
                    key, currentCount, maxCount
                )
            }
            return false
        }

        // 요청 기록
        window.recordRequest(currentTime)

        if (logger.isDebugEnabled) {
            logger.debug(
                "[SlidingWindow] Acquired permit for key: {}, count: {}/{}",
                key, currentCount + 1, maxCount
            )
        }

        return true
    }

    /**
     * @since: 2025-09-12
     * 현재 사용량 조회
     * @param key 제한 키
     * @param windowMs 윈도우 크기 (밀리초)
     * @author: 전요셉
     */
    fun getCurrentUsage(key: String, windowMs: Long = 1000L): Long {
        val window = windows[key] ?: return 0
        return window.getCount(System.currentTimeMillis(), windowMs)
    }

    /**
     * @since: 2025-09-12
     * 정확한 사용량 조회 (부분 버킷 포함)
     * @param key 제한 키
     * @param windowMs 윈도우 크기 (밀리초)
     * @author: 전요셉
     */
    fun getPreciseUsage(key: String, windowMs: Long = 1000L): Double {
        val window = windows[key] ?: return 0.0
        return window.getPreciseCount(System.currentTimeMillis(), windowMs)
    }

    /**
     * @since: 2025-09-12
     * 특정 키 리셋
     * @author: 전요셉
     */
    fun reset(key: String) {
        windows[key]?.reset()
        logger.info("[SlidingWindow] Reset rate limiter for key: {}", key)
    }

    /**
     * @since: 2025-09-12
     * 모든 상태 클리어
     * @author: 전요셉
     */
    fun clearAll() {
        val keyCount = windows.size
        windows.clear()
        logger.info("[SlidingWindow] Cleared all rate limiter states. Keys cleared: {}", keyCount)
    }

    /**
     * @since: 2025-09-12
     * 통계 정보 조회
     * @author: 전요셉
     */
    fun getStats(): Map<String, Any> {
        val currentTime = System.currentTimeMillis()
        val activeWindows = windows.entries.count { (_, window) ->
            window.getCount(currentTime, 1000L) > 0
        }

        val totalRequests = windows.values.sumOf { window ->
            window.getCount(currentTime, MAX_WINDOW_MS)
        }

        return mapOf(
            "totalKeys" to windows.size,
            "activeWindows" to activeWindows,
            "totalRequests" to totalRequests,
            "bucketSizeMs" to BUCKET_SIZE_MS,
            "numBuckets" to NUM_BUCKETS,
            "maxWindowMs" to MAX_WINDOW_MS
        )
    }

    /**
     * @since: 2025-09-12
     * 상세 통계 (디버깅용)
     * @param key 제한 키
     * @param windowMs 윈도우 크기 (밀리초)
     * @author: 전요셉
     */
    fun getDetailedStats(key: String, windowMs: Long = 1000L): Map<String, Any>? {
        val window = windows[key] ?: return null
        val currentTime = System.currentTimeMillis()

        return mapOf(
            "key" to key,
            "currentCount" to window.getCount(currentTime, windowMs),
            "preciseCount" to window.getPreciseCount(currentTime, windowMs),
            "windowMs" to windowMs,
            "lastUpdateTime" to window.lastUpdateTime,
            "timeSinceLastUpdate" to (currentTime - window.lastUpdateTime)
        )
    }
}
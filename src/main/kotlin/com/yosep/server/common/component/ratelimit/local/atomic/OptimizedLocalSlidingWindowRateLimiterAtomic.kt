package com.yosep.server.common.component.ratelimit.local.atomic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.LongAdder

/**
 * 고경합 최적화된 Lock-free Rate Limiter
 *
 * 핵심 최적화:
 * 1. Immutable state object로 CAS 효율 극대화
 * 2. Padding으로 false sharing 방지
 * 3. Adaptive backoff로 경합 시 CPU 효율 개선
 * 4. LongAdder로 통계 수집 (경합 회피)
 */
@Component
class OptimizedLocalSlidingWindowRateLimiterAtomic {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val windowStates = ConcurrentHashMap<String, PaddedAtomicReference>()

    // 통계용 (경합 없는 카운터)
    private val totalRequests = LongAdder()
    private val totalRetries = LongAdder()
    private val totalSuccesses = LongAdder()

    /**
     * Cache line padding to prevent false sharing
     * CPU 캐시 라인은 보통 64바이트
     */
    class PaddedAtomicReference(initial: WindowState) {
        @Volatile private var p0: Long = 0L  // padding
        @Volatile private var p1: Long = 0L
        @Volatile private var p2: Long = 0L
        @Volatile private var p3: Long = 0L
        @Volatile private var p4: Long = 0L
        @Volatile private var p5: Long = 0L
        @Volatile private var p6: Long = 0L

        private val ref = AtomicReference(initial)

        @Volatile private var p7: Long = 0L  // padding
        @Volatile private var p8: Long = 0L
        @Volatile private var p9: Long = 0L
        @Volatile private var p10: Long = 0L
        @Volatile private var p11: Long = 0L
        @Volatile private var p12: Long = 0L
        @Volatile private var p13: Long = 0L

        fun get(): WindowState = ref.get()
        fun compareAndSet(expect: WindowState, update: WindowState): Boolean =
            ref.compareAndSet(expect, update)
    }

    /**
     * Immutable state - CAS 효율 극대화
     */
    data class WindowState(
        val windowStart: Long,
        val count: Int
    ) {
        // 빠른 만료 체크를 위한 계산된 속성
        fun isExpired(currentTime: Long, windowMs: Long): Boolean =
            currentTime - windowStart >= windowMs

        fun canAcquire(maxCount: Int): Boolean =
            count < maxCount
    }

    /**
     * 최적화된 acquire - adaptive backoff
     */
    suspend fun tryAcquire(key: String, maxCount: Int, windowMs: Long): Boolean {
        totalRequests.increment()

        val stateRef = windowStates.computeIfAbsent(key) {
            PaddedAtomicReference(WindowState(System.currentTimeMillis(), 0))
        }

        var attempts = 0
        var backoffNanos = 0L

        while (attempts < 32) { // 2^5, 충분한 재시도
            attempts++

            val currentTime = System.currentTimeMillis()
            val current = stateRef.get()

            // Fast path: 윈도우 만료 체크
            if (current.isExpired(currentTime, windowMs)) {
                // 새 윈도우로 리셋
                val newState = WindowState(currentTime, 1)
                if (stateRef.compareAndSet(current, newState)) {
                    totalSuccesses.increment()
                    return true
                }
            } else if (current.canAcquire(maxCount)) {
                // 카운트 증가
                val newState = WindowState(current.windowStart, current.count + 1)
                if (stateRef.compareAndSet(current, newState)) {
                    totalSuccesses.increment()
                    return true
                }
            } else {
                // 한도 초과 - 빠른 실패
                return false
            }

            // CAS 실패 시 adaptive backoff
            totalRetries.increment()

            if (attempts <= 3) {
                // 초기 3회는 즉시 재시도 (spin)
                continue
            } else if (attempts <= 8) {
                // 4-8회는 짧은 pause
                Thread.onSpinWait() // CPU hint for spin-wait loop
            } else {
                // 그 이후는 exponential backoff
                backoffNanos = Math.min(backoffNanos * 2 + 100, 10_000)
                val endNanos = System.nanoTime() + backoffNanos
                while (System.nanoTime() < endNanos) {
                    Thread.onSpinWait()
                }
            }
        }

        logger.warn("Failed after {} attempts for key: {}", attempts, key)
        return false
    }

    /**
     * Wait-free read operation
     */
    fun getCurrentUsage(key: String): Int {
        val stateRef = windowStates[key] ?: return 0
        val state = stateRef.get()
        val currentTime = System.currentTimeMillis()

        return if (state.isExpired(currentTime, 1000)) {
            0
        } else {
            state.count
        }
    }

    /**
     * 성능 통계
     */
    fun getPerformanceStats(): Map<String, Any> {
        val requests = totalRequests.sum()
        val retries = totalRetries.sum()
        val successes = totalSuccesses.sum()

        return mapOf(
            "totalRequests" to requests,
            "totalRetries" to retries,
            "totalSuccesses" to successes,
            "avgRetriesPerRequest" to if (requests > 0) retries.toDouble() / requests else 0.0,
            "successRate" to if (requests > 0) successes.toDouble() / requests else 0.0,
            "activeWindows" to windowStates.size
        )
    }

    /**
     * Warm up - JIT 최적화를 위한 사전 실행
     */
    fun warmUp() {
        repeat(10000) {
            val key = "warmup"
            val state = windowStates.computeIfAbsent(key) {
                PaddedAtomicReference(WindowState(0L, 0))
            }
            state.get()
        }
        windowStates.clear()
    }
}
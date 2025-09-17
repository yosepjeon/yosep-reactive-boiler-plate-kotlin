package com.yosep.server.common.component.ratelimit.local.atomic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Lock-free version of LocalSlidingWindowRateLimiter using AtomicReference with CAS operations
 *
 * 장점:
 * 1. Lock-free: 스레드 블로킹 없이 높은 처리량 달성
 * 2. 낮은 지연시간: 락 대기 시간이 없어 응답 속도 향상
 * 3. Deadlock-free: 데드락 발생 가능성 완전 제거
 * 4. Fair scheduling: 모든 스레드가 공평하게 진행
 *
 * 단점:
 * 1. CPU 사용량 증가: 실패 시 재시도로 인한 busy-waiting
 * 2. 복잡도 증가: 코드 이해와 디버깅이 더 어려움
 * 3. ABA 문제 가능성: timestamp로 완화하지만 완전 해결은 어려움
 * 4. 높은 경쟁 상황에서 성능 저하: 재시도가 많아질 수 있음
 */
@Component
class LocalSlidingWindowRateLimiterAtomic {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val windowStates = ConcurrentHashMap<String, AtomicReference<WindowState>>()

    data class WindowState(
        val windowStart: Long,
        val count: Int,
        val version: Long = System.nanoTime() // ABA 문제 방지용 버전
    )

    /**
     * Rate limit 획득 시도 - CAS 기반 lock-free 구현
     * 실패 시 성공할 때까지 재시도
     */
    suspend fun tryAcquire(key: String, maxCount: Int, windowMs: Long): Boolean {
        val stateRef = windowStates.computeIfAbsent(key) {
            AtomicReference(WindowState(System.currentTimeMillis(), 0))
        }

        var attempts = 0
        val maxAttempts = 100 // 무한 루프 방지

        while (attempts < maxAttempts) {
            attempts++

            val currentTime = System.currentTimeMillis()
            val currentState = stateRef.get()

            // 윈도우 경계 체크
            val elapsedTime = currentTime - currentState.windowStart

            val newState = if (elapsedTime >= windowMs) {
                // 새 윈도우 시작
                WindowState(currentTime, 1)
            } else {
                // 현재 윈도우 내에서 카운트 체크
                if (currentState.count >= maxCount) {
                    // 한도 초과
                    if (logger.isDebugEnabled) {
                        logger.debug("[Atomic] Rate limit exceeded for key: {}, count: {}/{}",
                            key, currentState.count, maxCount)
                    }
                    return false
                }
                // 카운트 증가
                WindowState(
                    currentState.windowStart,
                    currentState.count + 1,
                    System.nanoTime()
                )
            }

            // CAS 연산 시도
            if (stateRef.compareAndSet(currentState, newState)) {
                if (logger.isDebugEnabled) {
                    logger.debug("[Atomic] Acquired permit for key: {} (attempts: {}), count: {}/{}",
                        key, attempts, newState.count, maxCount)
                }
                return true
            }

            // CAS 실패 시 짧은 백오프
            if (attempts > 10) {
                // 경쟁이 심한 경우 짧은 대기
                Thread.yield()
            }
        }

        logger.warn("[Atomic] Failed to acquire permit after {} attempts for key: {}", maxAttempts, key)
        return false
    }

    /**
     * 현재 사용량 조회 - lock-free read
     */
    fun getCurrentUsage(key: String): Int {
        val stateRef = windowStates[key] ?: return 0
        val state = stateRef.get()
        val currentTime = System.currentTimeMillis()

        // 윈도우가 만료되었으면 0 반환
        return if (currentTime - state.windowStart >= 1000) {
            0
        } else {
            state.count
        }
    }

    /**
     * 특정 키의 상태 리셋 - CAS 기반
     */
    fun reset(key: String) {
        val stateRef = windowStates[key] ?: return

        var attempts = 0
        while (attempts < 10) {
            attempts++
            val current = stateRef.get()
            val newState = WindowState(System.currentTimeMillis(), 0)

            if (stateRef.compareAndSet(current, newState)) {
                logger.info("[Atomic] Reset rate limiter for key: {} (attempts: {})", key, attempts)
                return
            }
        }

        logger.warn("[Atomic] Failed to reset rate limiter for key: {} after 10 attempts", key)
    }

    /**
     * 모든 상태 클리어
     */
    fun clearAll() {
        val keys = windowStates.keys.toList()
        windowStates.clear()
        logger.info("[Atomic] Cleared all rate limiter states. Keys cleared: {}", keys.size)
    }

    /**
     * 통계 정보 조회
     */
    fun getStats(): Map<String, Any> {
        val activeWindows = windowStates.entries.count { (_, ref) ->
            val state = ref.get()
            System.currentTimeMillis() - state.windowStart < 1000
        }

        return mapOf(
            "totalKeys" to windowStates.size,
            "activeWindows" to activeWindows
        )
    }
}
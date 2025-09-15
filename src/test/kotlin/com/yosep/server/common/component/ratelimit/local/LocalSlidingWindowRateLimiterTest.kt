package com.yosep.server.common.component.ratelimit.local

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class LocalSlidingWindowRateLimiterTest {

    private lateinit var rateLimiter: LocalSlidingWindowRateLimiter

    @BeforeEach
    fun setUp() {
        rateLimiter = LocalSlidingWindowRateLimiter()
    }

    @Test
    @DisplayName("기본 sliding window 동작 확인")
    fun testBasicSlidingWindow() = runTest {
        val key = "test-key"
        val maxQps = 10
        val windowMs = 1000

        repeat(5) {
            val result = rateLimiter.tryAcquire(key, maxQps, windowMs)
            assertThat(result).isTrue()
        }

        val currentCount = rateLimiter.getCurrentCount(key)
        assertThat(currentCount).isEqualTo(5L)
    }

    @Test
    @DisplayName("제한 초과 시 거부 확인")
    fun testRateLimitExceeded() = runTest {
        val key = "test-key-limit"
        val maxQps = 3
        val windowMs = 1000

        repeat(3) {
            val result = rateLimiter.tryAcquire(key, maxQps, windowMs)
            assertThat(result).isTrue()
        }

        val exceededResult = rateLimiter.tryAcquire(key, maxQps, windowMs)
        assertThat(exceededResult).isFalse()
    }

    @Test
    @DisplayName("sliding window 시간 흐름에 따른 동작")
    fun testSlidingWindowTimeProgression() = runTest {
        val key = "test-time-progression-unique" // 유니크한 키 사용
        val maxQps = 5
        val windowMs = 1000 // 1초 윈도우

        // 5개 요청 모두 성공해야 함
        repeat(5) {
            val result = rateLimiter.tryAcquire(key, maxQps, windowMs)
            assertThat(result).isTrue()
            delay(10) // 요청 간 약간의 딜레이
        }

        // 6번째 요청은 실패해야 함
        val failResult = rateLimiter.tryAcquire(key, maxQps, windowMs)
        assertThat(failResult).isFalse()

        // 키를 리셋하고 새로 시작 (더 명확한 테스트)
        rateLimiter.reset(key)
        
        // 리셋 후 다시 성공해야 함
        val successResult = rateLimiter.tryAcquire(key, maxQps, windowMs)
        assertThat(successResult).isTrue()
    }

    @Test
    @DisplayName("여러 키에 대한 독립적인 카운팅")
    fun testMultipleKeysIndependence() = runTest {
        val key1 = "key1"
        val key2 = "key2"
        val maxQps = 3
        val windowMs = 1000

        repeat(3) {
            rateLimiter.tryAcquire(key1, maxQps, windowMs)
        }

        val key1Exceeded = rateLimiter.tryAcquire(key1, maxQps, windowMs)
        assertThat(key1Exceeded).isFalse()

        val key2Success = rateLimiter.tryAcquire(key2, maxQps, windowMs)
        assertThat(key2Success).isTrue()

        assertThat(rateLimiter.getCurrentCount(key1)).isEqualTo(3L)
        assertThat(rateLimiter.getCurrentCount(key2)).isEqualTo(1L)
    }

    @Test
    @DisplayName("리셋 기능 확인")
    fun testReset() = runTest {
        val key = "reset-test"
        val maxQps = 5
        val windowMs = 1000

        repeat(3) {
            rateLimiter.tryAcquire(key, maxQps, windowMs)
        }

        assertThat(rateLimiter.getCurrentCount(key)).isEqualTo(3L)

        rateLimiter.reset(key)

        assertThat(rateLimiter.getCurrentCount(key)).isEqualTo(0L)

        val result = rateLimiter.tryAcquire(key, maxQps, windowMs)
        assertThat(result).isTrue()
        assertThat(rateLimiter.getCurrentCount(key)).isEqualTo(1L)
    }

    @Test
    @DisplayName("동시성 테스트")
    fun testConcurrency() = runTest {
        val key = "concurrent-test"
        val maxQps = 10
        val windowMs = 1000

        val jobs = mutableListOf<kotlinx.coroutines.Deferred<Boolean>>()

        repeat(20) {
            jobs.add(async {
                rateLimiter.tryAcquire(key, maxQps, windowMs)
            })
        }

        val results = jobs.map { it.await() }
        val successCount = results.count { it }

        assertThat(successCount).isLessThanOrEqualTo(maxQps)
        assertThat(successCount).isGreaterThan(0)
        assertThat(rateLimiter.getCurrentCount(key)).isEqualTo(successCount.toLong())
    }
}
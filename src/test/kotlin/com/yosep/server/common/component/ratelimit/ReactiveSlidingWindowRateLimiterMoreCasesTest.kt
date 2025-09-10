package com.yosep.server.common.component.ratelimit

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.config.RedisConfig
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles

/**
 * Integration tests for ReactiveSlidingWindowRateLimiter covering more cases:
 * - independent quotas per key
 * - strict boundary behavior
 */
@DataR2dbcTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [RedisConfig::class, ReactiveSlidingWindowRateLimiter::class])
class ReactiveSlidingWindowRateLimiterMoreCasesTest @Autowired constructor(
    private val limiter: ReactiveSlidingWindowRateLimiter,
) : AbstractIntegrationContainerBase() {

    @Test
    fun `서로 다른 키는 카운터가 독립적으로 동작한다`() = runBlocking {
        val windowMs = 400
        val maxQps = 2

        val k1 = "RL_INDEP_K1_${System.nanoTime()}"
        val k2 = "RL_INDEP_K2_${System.nanoTime()}"

        // First key acquires twice successfully
        val k1_a1 = limiter.tryAcquireSuspend(k1, maxQps, windowMs)
        val k1_a2 = limiter.tryAcquireSuspend(k1, maxQps, windowMs)
        val k1_a3 = limiter.tryAcquireSuspend(k1, maxQps, windowMs)

        // Second key should not be affected
        val k2_a1 = limiter.tryAcquireSuspend(k2, maxQps, windowMs)
        val k2_a2 = limiter.tryAcquireSuspend(k2, maxQps, windowMs)
        val k2_a3 = limiter.tryAcquireSuspend(k2, maxQps, windowMs)

        assertEquals(true, k1_a1)
        assertEquals(true, k1_a2)
        assertEquals(false, k1_a3)

        assertEquals(true, k2_a1)
        assertEquals(true, k2_a2)
        assertEquals(false, k2_a3)
    }

    @Test
    fun `경계조건 N개까지 허용·N+1은 거부되고 윈도우가 지나면 다시 허용된다`() = runBlocking {
        val windowMs = 300
        val maxQps = 3
        val key = "RL_BOUNDARY_${System.nanoTime()}"

        val a1 = limiter.tryAcquireSuspend(key, maxQps, windowMs)
        val a2 = limiter.tryAcquireSuspend(key, maxQps, windowMs)
        val a3 = limiter.tryAcquireSuspend(key, maxQps, windowMs)
        val a4 = limiter.tryAcquireSuspend(key, maxQps, windowMs)

        assertEquals(true, a1)
        assertEquals(true, a2)
        assertEquals(true, a3)
        assertEquals(false, a4)

        Thread.sleep(windowMs.toLong() + 100)

        val after = limiter.tryAcquireSuspend(key, maxQps, windowMs)
        assertEquals(true, after)
    }
}

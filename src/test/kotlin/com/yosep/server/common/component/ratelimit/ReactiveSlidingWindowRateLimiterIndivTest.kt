package com.yosep.server.common.component.ratelimit

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.config.RedisConfig
import com.yosep.server.infrastructure.redis.component.RedisCommandHelper
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles

/**
 * Individual test for ReactiveSlidingWindowRateLimiter using Redis Testcontainer.
 */
@DataR2dbcTest
@org.springframework.boot.autoconfigure.ImportAutoConfiguration(FlywayAutoConfiguration::class)
@ActiveProfiles("test")
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [RedisConfig::class, ReactiveSlidingWindowRateLimiter::class, RedisCommandHelper::class,])
class ReactiveSlidingWindowRateLimiterIndivTest(
    @Autowired private val limiter: ReactiveSlidingWindowRateLimiter,
) : AbstractIntegrationContainerBase() {

    @Autowired
    private lateinit var redisCommandHelper: RedisCommandHelper

    @Test
    fun `같은 윈도 내에서 한도까지는 허용되고 초과 시 차단된다`() = runTest {
        val key = "rate:sw:indiv:${System.nanoTime()}"
        val windowMs = 1000
        val max = 3

        val a1 = limiter.tryAcquireSuspend(key, max, windowMs)
        val a2 = limiter.tryAcquireSuspend(key, max, windowMs)
        val a3 = limiter.tryAcquireSuspend(key, max, windowMs)
        val a4 = limiter.tryAcquireSuspend(key, max, windowMs)

        assertTrue(a1)
        assertTrue(a2)
        assertTrue(a3)
        assertFalse(a4) // 4th within same window should be blocked
    }

    @Test
    fun `윈도 경과 후 다시 허용된다`() = runTest {
        val key = "rate:sw:indiv2:${System.nanoTime()}"
        val windowMs = 1000
        val max = 3

        repeat(max) {
            assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))
            delay(300) // 각 시도 사이에 약간의 지연 추가 (동시성 문제 완화)
        }
        assertFalse(limiter.tryAcquireSuspend(key, max, windowMs))

        // 최소 3~4 윈도우 + 버퍼 권장 (경계/시계 오차 고려)
        Thread.sleep(windowMs * 1L + 300)

        println("aquire count: ${redisCommandHelper.get(redisCommandHelper.keys()[0])}")
        assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))
    }

    @Test
    fun `경계 직후에는 1건 허용되고 다음 시도는 차단된다`() = runTest {
        val key = "rate:sw:indiv3:${System.nanoTime()}"
        val windowMs = 1000
        val max = 4

        // 0.9초 지점에 3건 실행: 현재 윈도우의 900ms 근처에 맞춘다
        val targetMs = 900L
        val tolerance = 40L  // 860~940ms 내에서 시도
        while (true) {
            val now = System.currentTimeMillis()
            val elapsed = (now % windowMs).toLong()
            if (elapsed in (targetMs - tolerance)..(targetMs + tolerance)) {
                break
            }
            val toBoundary = if (elapsed < targetMs - tolerance) {
                // 앞으로 이동
                (targetMs - tolerance) - elapsed
            } else {
                // 다음 윈도 타겟까지 대기
                windowMs - elapsed + (targetMs - tolerance)
            }
            delay(toBoundary)
        }

        // 타겟 영역에서 3건 시도 (동일 윈도에 카운팅되도록 빠르게 실행)
        assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))
        assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))
        assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))

        // 1.0초 경계 직후로 이동
        val epsilon = 10L
        run {
            val now = System.currentTimeMillis()
            val elapsed = (now % windowMs).toLong()
            val sleepMs = (windowMs - elapsed) + epsilon
            delay(sleepMs)
        }

        println("############### After boundary - first attempt should pass")
        println("count: ${redisCommandHelper.get(redisCommandHelper.keys()[0])}")

        // 경계 직후 첫 시도는 허용
        assertTrue(limiter.tryAcquireSuspend(key, max, windowMs))

        println("############### Immediately next - should be blocked")
        println("count: ${redisCommandHelper.get(redisCommandHelper.keys()[0])}")

        // 바로 다음 시도는 차단
        assertFalse(limiter.tryAcquireSuspend(key, max, windowMs))
    }

    @Test
    fun `서로 다른 키는 서로 간섭하지 않는다`() = runTest {
        val key1 = "rate:sw:indiv4:${System.nanoTime()}"
        val key2 = "rate:sw:indiv5:${System.nanoTime()}"
        val windowMs = 1000
        val max = 3

        // Saturate key1
        repeat(max) { assertTrue(limiter.tryAcquireSuspend(key1, max, windowMs)) }
        assertFalse(limiter.tryAcquireSuspend(key1, max, windowMs))

        // key2 should not be affected
        assertTrue(limiter.tryAcquireSuspend(key2, max, windowMs))
        assertTrue(limiter.tryAcquireSuspend(key2, max, windowMs))
    }
}

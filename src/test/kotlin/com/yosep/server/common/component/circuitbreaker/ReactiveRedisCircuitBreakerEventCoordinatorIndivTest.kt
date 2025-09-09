package com.yosep.server.common.component.circuitbreaker

import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.config.RedisConfig
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

/**
 * Individual slice test for ReactiveRedisCircuitBreakerEventCoordinator
 * - Brings only Redis + Coordinator + lightweight beans
 * - Verifies Redis bootstrap and state transition via Lua FSM
 */
@DataR2dbcTest
@org.springframework.boot.autoconfigure.ImportAutoConfiguration(org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration::class)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [com.yosep.server.infrastructure.db.config.MasterDBConfig::class, RedisConfig::class, ReactiveRedisCircuitBreakerEventCoordinator::class, ReactiveRedisCircuitBreakerEventCoordinatorIndivTest.TestBeans::class])
class ReactiveRedisCircuitBreakerEventCoordinatorIndivTest @Autowired constructor(
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val redisson: RedissonReactiveClient,
    private val cbRegistry: CircuitBreakerRegistry,
) : AbstractIntegrationContainerBase() {

    @Configuration
    class TestBeans {
        @Bean fun circuitBreakerRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        @Bean fun objectMapper(): ObjectMapper = ObjectMapper()
    }

    private fun newEntity(name: String) = CircuitBreakerConfigEntity(
        breakerName = name,
        failureRateThreshold = 20,
        slowCallRateThreshold = 20,
        slowCallDurationThresholdMs = 60_000,
        waitDurationInOpenStateMs = 5_000,
        permittedCallsInHalfOpenState = 1,
        minimumNumberOfCalls = 10,
        slidingWindowSize = 100,
        slidingWindowType = "COUNT_BASED",
        recordFailureStatusCodes = null,
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        isNew = true,
    )

    private suspend fun awaitState(name: String, timeoutMs: Long = 3000): Pair<String?, String?> {
        val key = "cb:{%s}:state".format(name)
        val map = redisson.getMap<String, String>(key)
        var waited = 0L
        val step = 100L
        while (waited < timeoutMs) {
            val state = map.get("state").awaitSingleOrNull()
            val version = map.get("version").awaitSingleOrNull()
            if (state != null && version != null) return state to version
            delay(step)
            waited += step
        }
        return null to null
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `registerCircuitBreaker 부트스트랩이 Redis 상태를 CLOSED로 초기화한다`() = runTest {
        val name = "TEST_CB_INDIV_${System.nanoTime()}"

        // when
        coordinator.registerCircuitBreaker(newEntity(name)).awaitSingleOrNull()
        // register triggers bootstrap asynchronously
        delay(200)

        // then: await Redis state map (bootstrap is async)
        val (state, version) = awaitState(name, 3000)
        assertEquals("CLOSED", state)
        assertNotNull(version)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `FSM 제안을 통해 CLOSED에서 OPEN으로 전환하고 동기화로 로컬 상태를 반영한다`() = runTest {
        val name = "TEST_CB_TRANSITION_${System.nanoTime()}"

        // given
        coordinator.registerCircuitBreaker(newEntity(name)).awaitSingleOrNull()
        delay(200)

        // when: propose transition via Lua FSM
        val ok = coordinator.proposeTransitionSuspend(name, from = "CLOSED", to = "OPEN")
        // ensure local registry reflects Redis via one-shot sync
        coordinator.syncFromRedisOncePublic(name)

        // then
        val cb = cbRegistry.circuitBreaker(name)
        assertEquals(true, ok)
        assertEquals("OPEN", cb.state.name)
    }
}

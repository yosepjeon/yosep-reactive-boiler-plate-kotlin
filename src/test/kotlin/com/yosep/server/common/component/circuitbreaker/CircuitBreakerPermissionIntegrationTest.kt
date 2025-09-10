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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

/**
 * Integration tests verifying CircuitBreaker permission behavior across states.
 */
@DataR2dbcTest
@org.springframework.boot.autoconfigure.ImportAutoConfiguration(FlywayAutoConfiguration::class)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [
    com.yosep.server.infrastructure.db.config.MasterDBConfig::class,
    RedisConfig::class,
    ReactiveRedisCircuitBreakerEventCoordinator::class,
    CircuitBreakerPermissionIntegrationTest.TestBeans::class
])
class CircuitBreakerPermissionIntegrationTest @Autowired constructor(
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val cbRegistry: CircuitBreakerRegistry,
) : AbstractIntegrationContainerBase() {

    @Configuration
    class TestBeans {
        @Bean fun circuitBreakerRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        @Bean fun objectMapper(): ObjectMapper = ObjectMapper()
    }

    private fun newEntity(name: String) = CircuitBreakerConfigEntity(
        breakerName = name,
        failureRateThreshold = 50,
        slowCallRateThreshold = 100,
        slowCallDurationThresholdMs = 1_000,
        waitDurationInOpenStateMs = 1_000,
        permittedCallsInHalfOpenState = 1, // allow only one permit in HALF_OPEN
        minimumNumberOfCalls = 10,
        slidingWindowSize = 10,
        slidingWindowType = "COUNT_BASED",
        recordFailureStatusCodes = null,
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        isNew = true,
    )

    private suspend fun registerAndBootstrap(name: String) {
        coordinator.registerCircuitBreaker(newEntity(name)).awaitSingleOrNull()
        // allow async bootstrap
        delay(200)
        coordinator.syncFromRedisOncePublic(name)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `CLOSED state allows permission`() = runTest {
        val name = "CB_PERM_CLOSED_${System.nanoTime()}"
        registerAndBootstrap(name)

        val cb = cbRegistry.circuitBreaker(name)
        // ensure CLOSED via explicit sync (bootstrap initializes to CLOSED)
        coordinator.syncFromRedisOncePublic(name)
        assertEquals("CLOSED", cb.state.name)

        val allowed = cb.tryAcquirePermission()
        assertEquals(true, allowed)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `OPEN state denies permission`() = runTest {
        val name = "CB_PERM_OPEN_${System.nanoTime()}"
        registerAndBootstrap(name)

        // CLOSED -> OPEN
        coordinator.proposeTransitionSuspend(name, from = "CLOSED", to = "OPEN")
        coordinator.syncFromRedisOncePublic(name)

        val cb = cbRegistry.circuitBreaker(name)
        assertEquals("OPEN", cb.state.name)
        val allowed = cb.tryAcquirePermission()
        assertEquals(false, allowed)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `HALF_OPEN permits only one concurrent call`() = runTest {
        val name = "CB_PERM_HALF_${System.nanoTime()}"
        registerAndBootstrap(name)

        // CLOSED -> OPEN -> HALF_OPEN
        coordinator.proposeTransitionSuspend(name, from = "CLOSED", to = "OPEN")
        coordinator.proposeTransitionSuspend(name, from = "OPEN", to = "HALF_OPEN")
        coordinator.syncFromRedisOncePublic(name)

        val cb = cbRegistry.circuitBreaker(name)
        assertEquals("HALF_OPEN", cb.state.name)

        val first = cb.tryAcquirePermission()
        val second = cb.tryAcquirePermission()

        assertEquals(true, first)
        assertEquals(false, second)
    }
}

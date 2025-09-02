package com.yosep.server.common.component.ratelimit

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.config.RedisConfig
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.redisson.api.RMapReactive
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

/**
 * Individual slice test for LatencyBasedReactiveAimdSlowStartRateCoordinator with n/m parameters.
 */
@DataR2dbcTest
@org.springframework.boot.autoconfigure.ImportAutoConfiguration(FlywayAutoConfiguration::class)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [com.yosep.server.infrastructure.db.config.MasterDBConfig::class, RedisConfig::class, LatencyBasedReactiveAimdSlowStartRateCoordinator::class])
class LatencyBasedReactiveAimdSlowStartRateCoordinatorIndivNnMTest(
    @Autowired private val coordinator: LatencyBasedReactiveAimdSlowStartRateCoordinator,
    @Autowired private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
    @Autowired private val redissonReactiveClient: RedissonReactiveClient,
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `비어있으면 getCurrentLimit가 기본값을 Redis 해시에 기록한다`() = runTest {
        val org = "TEST_ORG_DB_FALLBACK_LAT_INDIV_NM"
        val initialQps = 2345

        val entity = OrgRateLimitConfigEntity(
            org,
            initialQps,
            20000,
            100,
            500,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )
        orgRateLimitConfigRepository.save(entity)

        val limit = coordinator.getCurrentLimit(org)
        assertEquals(initialQps, limit)

        val map: RMapReactive<String, String> = redissonReactiveClient.getMap("rate:config:$org")

        val redisValue = map.get("limit_qps").awaitSingleOrNull()?.toInt()
        assertEquals(initialQps, redisValue)

    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `성공 시 n배로 증가하고 상한에선 m씩 증가한다`() = runTest {
        val org = "TEST_ORG_LAT_SUCCESS_INDIV_NM"
        val initialQps = 100
        val entity = OrgRateLimitConfigEntity(
            org,
            initialQps,
            20000,
            100,
            500,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )
        orgRateLimitConfigRepository.save(entity)

        val start = coordinator.getCurrentLimit(org)
        assertEquals(initialQps, start)

        // Use n=3, m=5
        val afterFirstSuccess = coordinator.reportSuccess(org = org, n = 3, m = 5, latencyMs = 10)
        assertEquals(300, afterFirstSuccess)

        val afterSecondSuccess = coordinator.reportSuccess(org = org, n = 3, m = 5, latencyMs = 10)
        assertEquals(900, afterSecondSuccess)

        // Prepare near cap and verify additive m up to max (100000)
        val map: RMapReactive<String, String> = redissonReactiveClient.getMap("rate:config:$org")
        map.fastPut("limit_qps", "99999").awaitSingleOrNull()

        val capped = coordinator.reportSuccess(org = org, n = 3, m = 5, latencyMs = 10)
        assertEquals(100_000, capped)

        val cappedAgain = coordinator.reportSuccess(org = org, n = 3, m = 5, latencyMs = 10)
        assertEquals(100_000, cappedAgain)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `실패 시 md로 감소하며 MIN_LIMIT 아래로 내려가지 않는다`() = runTest {
        val org = "TEST_ORG_LAT_FAILURE_INDIV_NM"
        val initialQps = 100
        val entity = OrgRateLimitConfigEntity(
            org,
            initialQps,
            20000,
            30,
            500,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )
        orgRateLimitConfigRepository.save(entity)

        val start = coordinator.getCurrentLimit(org)
        assertEquals(initialQps, start)

        val afterFirstFailure = coordinator.reportFailure(org = org, latencyMs = 800)
        assertEquals(50, afterFirstFailure)

        val afterSecondFailure = coordinator.reportFailure(org = org, latencyMs = 800)
        assertEquals(30, afterSecondFailure)

        val afterThirdFailure = coordinator.reportFailure(org = org, latencyMs = 800)
        assertEquals(30, afterThirdFailure)
    }
}

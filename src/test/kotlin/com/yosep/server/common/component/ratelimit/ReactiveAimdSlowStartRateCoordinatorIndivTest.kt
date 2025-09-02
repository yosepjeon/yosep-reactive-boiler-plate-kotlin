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
import org.redisson.api.RBucketReactive
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

/**
 * Lightweight slice test for ReactiveAimdSlowStartRateCoordinator
 * - Uses Testcontainers via AbstractIntegrationContainerBase
 * - Brings up only R2DBC slice + Redis beans + Coordinator bean
 * - Runs Flyway for schema creation via ImportAutoConfiguration on Flyway
 */
@DataR2dbcTest
@org.springframework.boot.autoconfigure.ImportAutoConfiguration(FlywayAutoConfiguration::class)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(value = [com.yosep.server.infrastructure.db.config.MasterDBConfig::class, RedisConfig::class, ReactiveAimdSlowStartRateCoordinator::class])
class ReactiveAimdSlowStartRateCoordinatorIndivTest(
    @Autowired private val coordinator: ReactiveAimdSlowStartRateCoordinator,
    @Autowired private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
    @Autowired private val redissonReactiveClient: RedissonReactiveClient,
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `Redis가 비어있으면 DB에서 조회하여 Redis에 기록한다`() = runTest {
        val org = "TEST_ORG_DB_FALLBACK_INDIV"
        val initialQps = 1234

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

        val bucket: RBucketReactive<String> = redissonReactiveClient.getBucket("rate:config:$org")
        val redisValue = bucket.get().awaitSingleOrNull()?.toInt()
        assertEquals(initialQps, redisValue)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `성공 시 슬로우스타트로 두 배 증가하고 MAX_LIMIT에서 상한된다`() = runTest {
        val org = "TEST_ORG_SUCCESS_INDIV"
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

        val afterFirstSuccess = coordinator.reportSuccess(org)
        assertEquals(200, afterFirstSuccess)

        val afterSecondSuccess = coordinator.reportSuccess(org)
        assertEquals(400, afterSecondSuccess)

        val bucket: RBucketReactive<String> = redissonReactiveClient.getBucket("rate:config:$org")
        bucket.set("99999").awaitSingleOrNull()
        val capped = coordinator.reportSuccess(org)
        assertEquals(100_000, capped)

        val cappedAgain = coordinator.reportSuccess(org)
        assertEquals(100_000, cappedAgain)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `실패 시 곱감으로 감소하되 MIN_LIMIT 아래로 내려가지 않는다`() = runTest {
        val org = "TEST_ORG_FAILURE_INDIV"
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

        val afterFirstFailure = coordinator.reportFailure(org)
        assertEquals(50, afterFirstFailure)

        val afterSecondFailure = coordinator.reportFailure(org)
        assertEquals(30, afterSecondFailure)

        val afterThirdFailure = coordinator.reportFailure(org)
        assertEquals(30, afterThirdFailure)
    }
}

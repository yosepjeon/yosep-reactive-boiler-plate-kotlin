package com.yosep.server.common.component.ratelimit

import com.yosep.server.common.AbstractIntegrationContainerBase
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
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactiveAimdSlowStartRateCoordinatorIntegTest(
    @Autowired private val coordinator: ReactiveAimdSlowStartRateCoordinator,
    @Autowired private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
    @Autowired private val redissonReactiveClient: RedissonReactiveClient,
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `Redis가 비어있으면 DB에서 조회하여 Redis에 기록한다`() = runTest {
        val org = "TEST_ORG_DB_FALLBACK"
        val initialQps = 1234

        // Prepare DB row
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

        // When: get current limit
        val limit = coordinator.getCurrentLimit(org)

        // Then: returns DB initialQps and writes to Redis
        assertEquals(initialQps, limit)

        val bucket: RBucketReactive<String> = redissonReactiveClient.getBucket("rate:config:$org")
        val redisValue = bucket.get().awaitSingleOrNull()?.toInt()
        assertEquals(initialQps, redisValue)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `성공 시 슬로우스타트로 두 배 증가하고 MAX_LIMIT에서 상한된다`() = runTest {
        val org = "TEST_ORG_SUCCESS"
        // Seed DB so getCurrentLimit will write initial value to Redis
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

        // Seed Redis via coordinator
        val start = coordinator.getCurrentLimit(org)
        assertEquals(initialQps, start)

        // First success: doubles 100 -> 200
        val afterFirstSuccess = coordinator.reportSuccess(org)
        assertEquals(200, afterFirstSuccess)

        // Second success: doubles 200 -> 400
        val afterSecondSuccess = coordinator.reportSuccess(org)
        assertEquals(400, afterSecondSuccess)

        // Bump value near max and ensure capping at 100_000
        // Directly set Redis to 99_999 and invoke success
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
        val org = "TEST_ORG_FAILURE"
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

        // Seed Redis via coordinator
        val start = coordinator.getCurrentLimit(org)
        assertEquals(initialQps, start)

        // First failure: floor(100*0.5) = 50
        val afterFirstFailure = coordinator.reportFailure(org)
        assertEquals(50, afterFirstFailure)

        // Second failure: floor(50*0.5) = 25 but min is 30 => 30
        val afterSecondFailure = coordinator.reportFailure(org)
        assertEquals(30, afterSecondFailure)

        // Another failure should keep it at min 30
        val afterThirdFailure = coordinator.reportFailure(org)
        assertEquals(30, afterThirdFailure)
    }
}

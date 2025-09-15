package com.yosep.server.common.component.ratelimit.local

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import kotlinx.coroutines.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test", "local")
@TestPropertySource(properties = [
    "ratelimit.local.enabled=true",
    "ratelimit.local.window-ms=1000",
    "ratelimit.local.sync-interval-ms=500",
    "ratelimit.local.enable-sync=true",
    "cb.local.peers=http://localhost:20000,http://localhost:20001,http://localhost:20002"
])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LocalRateLimitServiceIntegrationTest : AbstractIntegrationContainerBase() {

    @Autowired
    private lateinit var localRateLimitService: LocalRateLimitService

    @Autowired
    private lateinit var orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository

    @Autowired
    private lateinit var syncScheduler: LocalRateLimitSyncScheduler

    private val testOrgId = "test-org-1"

    @BeforeEach
    fun setUp() = runBlocking {
        orgRateLimitConfigRepository.deleteAll()
        
        val config = OrgRateLimitConfigEntity(
            id = testOrgId,
            initialQps = 1500,
            maxQps = 1500,
            minQps = 100,
            latencyThreshold = 500,
            createdAt = LocalDateTime.now(),
            updatedAt = LocalDateTime.now(),
            isNew = true
        )
        orgRateLimitConfigRepository.save(config)
        
        syncScheduler.forceSyncNow()
        delay(100)
    }

    @Test
    @DisplayName("기본 rate limiting 동작 확인")
    fun testBasicRateLimiting() = runBlocking {
        val result = localRateLimitService.getCurrentLimit(testOrgId)
        assertThat(result.success).isTrue()
        assertThat(result.nodeCount).isEqualTo(3) // 3개 노드 설정
        assertThat(result.perNodeLimit).isEqualTo(500) // 1500 / 3 = 500
        
        val acquired = localRateLimitService.tryAcquire(testOrgId)
        assertThat(acquired.acquired).isTrue()
        assertThat(acquired.perNodeLimit).isEqualTo(500)
    }

    @Test
    @DisplayName("노드 수 기반 배분 확인")
    fun testNodeCountBasedDistribution() = runBlocking {
        val limit = localRateLimitService.getCurrentLimit(testOrgId)
        
        assertThat(limit.currentLimit).isEqualTo(1500)
        assertThat(limit.perNodeLimit).isEqualTo(500) // 1500 / 3 nodes = 500 per node
        assertThat(limit.nodeCount).isEqualTo(3)
    }

    @Test
    @DisplayName("sliding window rate limiting 확인")
    fun testSlidingWindowRateLimiting() = runBlocking {
        var successCount = 0
        
        repeat(550) {
            val result = localRateLimitService.tryAcquire(testOrgId)
            if (result.acquired) {
                successCount++
            }
        }
        
        assertThat(successCount).isLessThanOrEqualTo(500)
        assertThat(successCount).isGreaterThan(450) // 일부 여유 허용
    }

    @Test
    @DisplayName("AIMD 성공 시 증가 확인")
    fun testAimdSuccess() = runBlocking {
        val initialLimit = localRateLimitService.getCurrentLimit(testOrgId)
        assertThat(initialLimit.currentLimit).isEqualTo(1500)
        
        val result = localRateLimitService.reportSuccess(testOrgId, latencyMs = 100)
        assertThat(result.success).isTrue()
        assertThat(result.newLimit).isGreaterThan(initialLimit.currentLimit)
        
        val newLimit = localRateLimitService.getCurrentLimit(testOrgId)
        assertThat(newLimit.currentLimit).isEqualTo(result.newLimit)
    }

    @Test
    @DisplayName("AIMD latency 기반 실패 처리 확인")
    fun testAimdLatencyBasedFailure() = runBlocking {
        val initialLimit = localRateLimitService.getCurrentLimit(testOrgId)
        
        val result = localRateLimitService.reportSuccess(testOrgId, latencyMs = 600)
        assertThat(result.success).isTrue()
        assertThat(result.adjustmentReason).isEqualTo("latency_failure")
        assertThat(result.newLimit).isLessThan(initialLimit.currentLimit)
    }

    @Test
    @DisplayName("DB 동기화 확인")
    fun testDbSync() = runBlocking {
        val newConfig = OrgRateLimitConfigEntity(
            id = "new-org",
            initialQps = 3000,
            maxQps = 3000,
            minQps = 200,
            latencyThreshold = 400,
            createdAt = LocalDateTime.now(),
            updatedAt = LocalDateTime.now(),
            isNew = true
        )
        orgRateLimitConfigRepository.save(newConfig)
        
        syncScheduler.forceSyncNow()
        delay(200)
        
        val result = localRateLimitService.getCurrentLimit("new-org")
        assertThat(result.success).isTrue()
        assertThat(result.perNodeLimit).isEqualTo(1000) // 3000 / 3 = 1000
    }

    @Test
    @DisplayName("현재 사용량 확인")
    fun testCurrentCount() = runBlocking {
        repeat(10) {
            localRateLimitService.tryAcquire(testOrgId)
        }
        
        val count = localRateLimitService.getCurrentCount(testOrgId)
        assertThat(count).isEqualTo(10L)
    }

    @Test
    @DisplayName("rate limit 리셋 확인")
    fun testRateLimitReset() = runBlocking {
        repeat(5) {
            localRateLimitService.tryAcquire(testOrgId)
        }
        
        val beforeReset = localRateLimitService.getCurrentCount(testOrgId)
        assertThat(beforeReset).isEqualTo(5L)
        
        localRateLimitService.resetRateLimit(testOrgId)
        
        val afterReset = localRateLimitService.getCurrentCount(testOrgId)
        assertThat(afterReset).isEqualTo(0L)
    }

    @Test
    @DisplayName("서비스 상태 확인")
    fun testServiceStatus() = runBlocking {
        val status = localRateLimitService.getServiceStatus()
        
        assertThat(status["localRateLimitEnabled"]).isEqualTo(true)
        assertThat(status["nodeCount"]).isEqualTo(3)
        
        @Suppress("UNCHECKED_CAST")
        val config = status["configuration"] as Map<String, Any>
        assertThat(config["windowMs"]).isEqualTo(1000)
        assertThat(config["latencyThresholdMs"]).isEqualTo(500L)
    }

    @Test
    @DisplayName("동시성 테스트")
    fun testConcurrency() = runBlocking {
        val jobs = mutableListOf<kotlinx.coroutines.Deferred<Boolean>>()
        
        repeat(100) {
            jobs.add(async {
                val result = localRateLimitService.tryAcquire(testOrgId)
                result.acquired
            })
        }
        
        val results = jobs.map { it.await() }
        val successCount = results.count { it }
        
        assertThat(successCount).isLessThanOrEqualTo(500)
        assertThat(successCount).isGreaterThan(0)
    }
}
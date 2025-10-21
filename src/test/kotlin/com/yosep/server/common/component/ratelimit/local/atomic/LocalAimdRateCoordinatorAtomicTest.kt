package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.mockk
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class LocalAimdRateCoordinatorAtomicTest {

    private lateinit var coordinator: LocalAimdRateCoordinatorAtomic
    private lateinit var properties: LocalRateLimitProperties

    @BeforeEach
    fun setUp() {
        properties = LocalRateLimitProperties()
        properties.maxLimit = 1000
        properties.minLimit = 10
        properties.targetP99Ms = 100L
        properties.addStep = 10
        properties.decreaseFactor = 0.5
        properties.reservoirSize = 1000
        properties.latencyWindowMs = 5000L
        properties.minSamplesForP99 = 10

        val mockK8sClient = mockk<KubernetesClient>(relaxed = true)
        coordinator = LocalAimdRateCoordinatorAtomic(
            k8s = mockK8sClient,
            properties = properties,
            meshSvc = "test-service",
            totalTargetQps = 0
        )
    }

    @AfterEach
    fun tearDown() {
        coordinator.shutdown()
    }

    @Test
    fun `초기화 테스트 - 여러 조직 설정 로드`() = runTest {
        // Given
        val configs = listOf(
            OrgRateLimitConfigEntity("org1", 100, 1000, 10, 200, null, null, true),
            OrgRateLimitConfigEntity("org2", 200, 2000, 20, 200, null, null, true),
            OrgRateLimitConfigEntity("org3", 50, 500, 5, 200, null, null, true)
        )

        // When
        coordinator.initializeFromConfig(configs)

        // Then
        assertThat(coordinator.getCurrentLimit("org1")).isEqualTo(100)
        assertThat(coordinator.getCurrentLimit("org2")).isEqualTo(200)
        assertThat(coordinator.getCurrentLimit("org3")).isEqualTo(50)

        val state1 = coordinator.getState("org1")
        assertThat(state1).isNotNull
        assertThat(state1?.limit).isEqualTo(100)
        assertThat(state1?.lastP99).isEqualTo(0L)
    }

    @Test
    fun `초기화 테스트 - 경계값 처리`() = runTest {
        // Given
        val configs = listOf(
            OrgRateLimitConfigEntity("org1", 5000, 10000, 100, 200, null, null, true), // maxLimit 초과
            OrgRateLimitConfigEntity("org2", 5, 1000, 1, 200, null, null, true)     // minLimit 미만
        )

        // When
        coordinator.initializeFromConfig(configs)

        // Then
        assertThat(coordinator.getCurrentLimit("org1")).isEqualTo(properties.maxLimit)
        assertThat(coordinator.getCurrentLimit("org2")).isEqualTo(properties.minLimit)
    }

    @Test
    fun `latency 기록 테스트 - 성공과 실패 모두 기록`() = runTest {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 1000, 10, 200, null, null, true)
        ))

        // When
        repeat(50) {
            coordinator.onSuccess(org, 50L + it)
        }
        repeat(50) {
            coordinator.onFailure(org, 150L + it)
        }

        // Then - 통계에 반영되는지 간접 확인
        delay(100) // 기록 완료 대기
        val stats = coordinator.getStatistics()
        @Suppress("UNCHECKED_CAST")
        val orgs = stats["organizations"] as Map<String, Any>
        assertThat(orgs).containsKey(org)
    }

    @Test
    fun `P99 기반 limit 증가 테스트`() = runTest {
        // Given
        val org = "testOrg"
        val initialLimit = 100
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, initialLimit, 1000, 10, 200, null, null, true)
        ))

        // 낮은 latency 기록 (target P99 = 100ms 이하)
        val now = System.currentTimeMillis()
        repeat(100) {
            coordinator.onSuccess(org, (30L..80L).random(), now - it * 10)
        }

        val limitBefore = coordinator.getCurrentLimit(org)

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val limitAfter = coordinator.getCurrentLimit(org)
        assertThat(limitAfter).isGreaterThan(limitBefore)
        assertThat(limitAfter).isEqualTo(limitBefore + properties.addStep)
    }

    @Test
    fun `P99 기반 limit 감소 테스트`() = runTest {
        // Given
        val org = "testOrg"
        val initialLimit = 200
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, initialLimit, 1000, 10, 200, null, null, true)
        ))

        // 높은 latency 기록 (target P99 = 100ms 초과)
        val now = System.currentTimeMillis()
        repeat(100) {
            coordinator.onSuccess(org, (150L..300L).random(), now - it * 10)
        }

        val limitBefore = coordinator.getCurrentLimit(org)

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val limitAfter = coordinator.getCurrentLimit(org)
        assertThat(limitAfter).isLessThan(limitBefore)
        assertThat(limitAfter).isEqualTo((limitBefore * properties.decreaseFactor).toInt())
    }

    @Test
    fun `최소 샘플 수 미달 시 조정 없음`() = runTest {
        // Given
        val org = "testOrg"
        val initialLimit = 100
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, initialLimit, 1000, 10, 200, null, null, true)
        ))

        // 최소 샘플 수보다 적게 기록
        repeat(5) { // minSamplesForP99 = 10
            coordinator.onSuccess(org, 200L)
        }

        val limitBefore = coordinator.getCurrentLimit(org)

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val limitAfter = coordinator.getCurrentLimit(org)
        assertThat(limitAfter).isEqualTo(limitBefore) // 변경 없음
    }

    @Test
    fun `시간 윈도우 외 샘플은 P99 계산에서 제외`() = runTest {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 1000, 10, 200, null, null, true)
        ))

        val now = System.currentTimeMillis()

        // 오래된 샘플 (윈도우 밖)
        repeat(50) {
            coordinator.onSuccess(org, 300L, now - properties.latencyWindowMs - 1000 - it * 10)
        }

        // 최근 샘플 (윈도우 안)
        repeat(50) {
            coordinator.onSuccess(org, 50L, now - it * 10)
        }

        val limitBefore = coordinator.getCurrentLimit(org)

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val limitAfter = coordinator.getCurrentLimit(org)
        // 최근 샘플들만 고려되므로 limit 증가
        assertThat(limitAfter).isGreaterThan(limitBefore)
    }

    @Test
    fun `여러 조직 독립적 조정`() = runTest {
        // Given
        val configs = listOf(
            OrgRateLimitConfigEntity("org1", 100, 1000, 10, 200, null, null, true),
            OrgRateLimitConfigEntity("org2", 100, 1000, 10, 200, null, null, true),
            OrgRateLimitConfigEntity("org3", 100, 1000, 10, 200, null, null, true)
        )
        coordinator.initializeFromConfig(configs)

        val now = System.currentTimeMillis()

        // org1: 낮은 latency
        repeat(50) {
            coordinator.onSuccess("org1", (30L..50L).random(), now - it * 10)
        }

        // org2: 높은 latency
        repeat(50) {
            coordinator.onSuccess("org2", (200L..300L).random(), now - it * 10)
        }

        // org3: 샘플 없음

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        assertThat(coordinator.getCurrentLimit("org1")).isGreaterThan(100) // 증가
        assertThat(coordinator.getCurrentLimit("org2")).isLessThan(100)    // 감소
        assertThat(coordinator.getCurrentLimit("org3")).isEqualTo(100)     // 변경 없음
    }

    @Test
    fun `주기적 조정 동작 테스트`() = runTest {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 1000, 10, 200, null, null, true)
        ))

        // 낮은 latency 지속 기록
        val job = launch {
            repeat(1000) {
                coordinator.onSuccess(org, (30L..50L).random())
                delay(10)
            }
        }

        // When
        coordinator.startPeriodicAdjustment(properties.latencyWindowMs)
        delay(3500) // 3.5초 대기 (3번 조정)

        // Then
        val finalLimit = coordinator.getCurrentLimit(org)
        // 주기적 조정이 실행되었는지만 확인 (증가 또는 유지)
        assertThat(finalLimit).isGreaterThanOrEqualTo(100)

        job.cancel()
    }

    @Test
    fun `동시성 테스트 - 여러 스레드에서 latency 기록`() = runTest {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 1000, 10, 200, null, null, true)
        ))

        val numThreads = 10
        val recordsPerThread = 100
        val barrier = CyclicBarrier(numThreads)
        val latch = CountDownLatch(numThreads)

        // When
        repeat(numThreads) { threadId ->
            launch(Dispatchers.IO) {
                barrier.await()
                repeat(recordsPerThread) {
                    runBlocking {
                        if (threadId % 2 == 0) {
                            coordinator.onSuccess(org, (50L..100L).random())
                        } else {
                            coordinator.onFailure(org, (100L..150L).random())
                        }
                    }
                }
                latch.countDown()
            }
        }

        latch.await()
        delay(100) // 모든 기록 완료 대기

        // Then
        coordinator.adjustOnce(properties.latencyWindowMs)
        val state = coordinator.getState(org)
        assertThat(state).isNotNull
        assertThat(state?.lastP99).isGreaterThan(0)
    }

    @Test
    fun `통계 정보 조회 테스트`() = runTest {
        // Given
        val configs = listOf(
            OrgRateLimitConfigEntity("org1", 100, 1000, 10, 200, null, null, true),
            OrgRateLimitConfigEntity("org2", 200, 2000, 20, 200, null, null, true)
        )
        coordinator.initializeFromConfig(configs)

        // 샘플 기록
        repeat(50) {
            coordinator.onSuccess("org1", 80L)
            coordinator.onSuccess("org2", 120L)
        }

        coordinator.adjustOnce(properties.latencyWindowMs)

        // When
        val stats = coordinator.getStatistics()

        // Then
        assertThat(stats).containsKey("organizations")
        assertThat(stats).containsKey("totalOrgs")
        assertThat(stats["totalOrgs"]).isEqualTo(2)

        @Suppress("UNCHECKED_CAST")
        val orgs = stats["organizations"] as Map<String, Map<String, Any>>
        assertThat(orgs).containsKeys("org1", "org2")
        assertThat(orgs["org1"]).containsKeys("limit", "lastP99Ms")
        assertThat(orgs["org2"]).containsKeys("limit", "lastP99Ms")
    }

    @Test
    fun `최대 limit 도달 시 더 이상 증가하지 않음`() = runTest {
        // Given
        val org = "testOrg"
        val nearMaxLimit = properties.maxLimit - 5
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, nearMaxLimit, 1000, 10, 200, null, null, true)
        ))

        // 낮은 latency로 증가 유도
        repeat(100) {
            coordinator.onSuccess(org, 30L)
        }

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val newLimit = coordinator.getCurrentLimit(org)
        assertThat(newLimit).isEqualTo(properties.maxLimit)

        // 추가 조정에도 maxLimit 유지
        repeat(100) {
            coordinator.onSuccess(org, 30L)
        }
        coordinator.adjustOnce(properties.latencyWindowMs)

        assertThat(coordinator.getCurrentLimit(org)).isEqualTo(properties.maxLimit)
    }

    @Test
    fun `최소 limit 도달 시 더 이상 감소하지 않음`() = runTest {
        // Given
        val org = "testOrg"
        val nearMinLimit = properties.minLimit + 5
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, nearMinLimit, 1000, 10, 200, null, null, true)
        ))

        // 높은 latency로 감소 유도
        repeat(100) {
            coordinator.onSuccess(org, 500L)
        }

        // When
        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val newLimit = coordinator.getCurrentLimit(org)
        assertThat(newLimit).isEqualTo(properties.minLimit)

        // 추가 조정에도 minLimit 유지
        repeat(100) {
            coordinator.onSuccess(org, 500L)
        }
        coordinator.adjustOnce(properties.latencyWindowMs)

        assertThat(coordinator.getCurrentLimit(org)).isEqualTo(properties.minLimit)
    }

    @Test
    fun `동적 조직 추가 테스트`() = runTest {
        // Given - 초기 조직
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity("org1", 100, 1000, 10, 200, null, null, true)
        ))

        // When - 새 조직에 대한 latency 기록 (자동 생성)
        repeat(50) {
            coordinator.onSuccess("org2", 75L)
        }

        coordinator.adjustOnce(properties.latencyWindowMs)

        // Then
        val limit1 = coordinator.getCurrentLimit("org1")
        val limit2 = coordinator.getCurrentLimit("org2")

        assertThat(limit1).isEqualTo(100) // 기존 조직 유지
        assertThat(limit2).isGreaterThan(0) // 새 조직 생성

        val state2 = coordinator.getState("org2")
        assertThat(state2).isNotNull
    }

    @Test
    fun `shutdown 테스트`() = runTest {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 1000, 10, 200, null, null, true)
        ))

        coordinator.startPeriodicAdjustment()
        delay(100)

        // When
        coordinator.shutdown()
        delay(1500) // 정상이면 조정이 중단됨

        val limitBefore = coordinator.getCurrentLimit(org)

        // 샘플 추가
        repeat(100) {
            coordinator.onSuccess(org, 30L)
        }

        delay(1500)

        // Then
        val limitAfter = coordinator.getCurrentLimit(org)
        assertThat(limitAfter).isEqualTo(limitBefore) // 조정 중단됨
    }
}
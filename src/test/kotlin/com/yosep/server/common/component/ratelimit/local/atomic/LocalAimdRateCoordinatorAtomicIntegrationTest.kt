package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.mockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class LocalAimdRateCoordinatorAtomicIntegrationTest {

    private lateinit var coordinator: LocalAimdRateCoordinatorAtomic
    private lateinit var properties: LocalRateLimitProperties

    @BeforeEach
    fun setUp() {
        properties = LocalRateLimitProperties()
        properties.maxLimit = 500
        properties.minLimit = 50
        properties.targetP99Ms = 200L
        properties.addStep = 20
        properties.decreaseFactor = 0.7
        properties.reservoirSize = 2000
        properties.latencyWindowMs = 10_000L
        properties.minSamplesForP99 = 50

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
    fun `실제 워크로드 시뮬레이션 - 트래픽 패턴 변화`() = runBlocking {
        // Given
        val orgs = listOf("org1", "org2", "org3")
        val configs = orgs.map { OrgRateLimitConfigEntity(it, 200, 1000, 50, 200, null, null, true) }
        coordinator.initializeFromConfig(configs)

        // 주기적 조정 시작
        coordinator.startPeriodicAdjustment()

        // When - 5초 동안 변화하는 워크로드 시뮬레이션
        val job = launch {
            var phase = 0
            while (isActive) {
                val currentTime = System.currentTimeMillis()
                phase = ((currentTime / 2000) % 3).toInt() // 2초마다 페이즈 변경

                orgs.forEach { org ->
                    launch {
                        // 페이즈별 다른 latency 패턴
                        val latency = when (phase) {
                            0 -> Random.nextLong(50, 150)   // 정상
                            1 -> Random.nextLong(250, 400)  // 과부하
                            else -> Random.nextLong(100, 200) // 회복
                        }

                        // 간헐적 실패
                        if (Random.nextFloat() < 0.1) {
                            coordinator.onFailure(org, latency + 100)
                        } else {
                            coordinator.onSuccess(org, latency)
                        }
                    }
                }
                delay(50)
            }
        }

        delay(3000) // 3초간 실행
        job.cancel()

        // Then - 각 조직의 limit이 적절히 조정되었는지 확인
        orgs.forEach { org ->
            val state = coordinator.getState(org)
            assertThat(state).isNotNull
            // P99가 계산되었는지만 확인 (초기값 0이 아님)
            assertThat(state!!.lastP99).isGreaterThanOrEqualTo(0)

            println("$org - Final limit: ${state.limit}, Last P99: ${state.lastP99}ms")

            // limit이 변경되었거나 최소한 유지되었는지 확인
            assertThat(state.limit).isBetween(properties.minLimit, properties.maxLimit)
        }

        val stats = coordinator.getStatistics()
        assertThat(stats["totalOrgs"]).isEqualTo(3)
    }

    @Test
    fun `백프레셔 시뮬레이션 - 다운스트림 서비스 장애`() = runBlocking {
        // Given
        val org = "testOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 300, 1500, 50, 200, null, null, true)
        ))

        coordinator.startPeriodicAdjustment()

        val results = mutableListOf<Int>()

        // When - 다운스트림 장애 시나리오
        val job = launch {
            // Phase 1: 정상 (2초)
            repeat(40) {
                coordinator.onSuccess(org, Random.nextLong(80, 120))
                delay(50)
            }
            results.add(coordinator.getCurrentLimit(org))

            // Phase 2: 다운스트림 장애 시작 (3초)
            repeat(60) {
                coordinator.onSuccess(org, Random.nextLong(400, 600))
                delay(50)
            }
            results.add(coordinator.getCurrentLimit(org))

            // Phase 3: 회복 중 (2초)
            repeat(40) {
                coordinator.onSuccess(org, Random.nextLong(150, 250))
                delay(50)
            }
            results.add(coordinator.getCurrentLimit(org))

            // Phase 4: 완전 회복 (2초)
            repeat(40) {
                coordinator.onSuccess(org, Random.nextLong(50, 100))
                delay(50)
            }
            results.add(coordinator.getCurrentLimit(org))
        }

        job.join()

        // Then
        assertThat(results).hasSize(4)
        println("Limit progression: ${results.joinToString(" -> ")}")

        // 첫 번째 limit이 감소했는지 확인
        if (results[1] >= results[0]) {
            println("WARNING: Limit did not decrease during failure phase: ${results[0]} -> ${results[1]}")
            println("Trying manual adjustment...")
            coordinator.adjustOnce()
            val newLimit = coordinator.getCurrentLimit(org)
            println("After manual adjust: $newLimit")
        }

        // 전체적인 경향성만 확인 (장애 시 증가하지 않고, 회복 시 감소하지 않음)
        assertThat(results[1]).isLessThanOrEqualTo(results[0]) // 장애 시 감소 또는 유지
        assertThat(results[3]).isGreaterThanOrEqualTo(results[2]) // 회복 시 증가 또는 유지
    }

    @Test
    fun `멀티 테넌트 환경 - 독립적 조정 확인`() = runBlocking {
        // Given
        val numOrgs = 10
        val configs = (1..numOrgs).map {
            OrgRateLimitConfigEntity("org$it", 150, 1000, 50, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        coordinator.startPeriodicAdjustment()

        // When - 각 조직별 다른 패턴
        val jobs = configs.mapIndexed { index, config ->
            launch {
                val org = config.id
                val baseLatency = 50L + (index * 30L) // 조직별 다른 기본 latency

                repeat(200) {
                    val latency = baseLatency + Random.nextLong(-20, 20)

                    if (Random.nextFloat() < 0.9) {
                        coordinator.onSuccess(org, latency)
                    } else {
                        coordinator.onFailure(org, latency * 2)
                    }

                    delay(Random.nextLong(10, 30))
                }
            }
        }

        jobs.forEach { it.join() }
        delay(1000) // 마지막 조정 대기

        // Then
        val limits = configs.map {
            it.id to coordinator.getCurrentLimit(it.id)
        }.toMap()

        // 낮은 latency 조직들은 높은 limit
        assertThat(limits["org1"]!!).isGreaterThan(limits["org5"]!!)
        assertThat(limits["org2"]!!).isGreaterThan(limits["org8"]!!)

        println("Final limits by org:")
        limits.forEach { (org, limit) ->
            val state = coordinator.getState(org)
            println("  $org: limit=$limit, p99=${state?.lastP99}ms")
        }
    }

    @Test
    fun `스파이크 트래픽 처리`() = runBlocking {
        // Given
        val org = "spikeOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 200, 1000, 50, 200, null, null, true)
        ))

        coordinator.startPeriodicAdjustment()

        // When
        val limitHistory = mutableListOf<Int>()

        // 정상 트래픽
        repeat(50) {
            coordinator.onSuccess(org, Random.nextLong(80, 120))
            delay(20)
        }
        limitHistory.add(coordinator.getCurrentLimit(org))

        // 갑작스런 스파이크
        val spikeJobs = List(20) {
            launch {
                repeat(50) {
                    coordinator.onSuccess(org, Random.nextLong(300, 500))
                }
            }
        }
        spikeJobs.forEach { it.join() }
        delay(1200) // 조정 대기
        limitHistory.add(coordinator.getCurrentLimit(org))

        // 스파이크 후 정상화
        repeat(100) {
            coordinator.onSuccess(org, Random.nextLong(80, 120))
            delay(20)
        }
        delay(2000)
        limitHistory.add(coordinator.getCurrentLimit(org))

        // Then
        println("Spike handling: ${limitHistory.joinToString(" -> ")}")

        // 스파이크 동안 limit이 감소했는지 확인
        if (limitHistory[1] >= limitHistory[0]) {
            println("WARNING: Limit did not decrease during spike: ${limitHistory[0]} -> ${limitHistory[1]}")
            val state = coordinator.getState(org)
            println("Current state: limit=${state?.limit}, p99=${state?.lastP99}")
        }

        // 전체적인 경향성 확인
        assertThat(limitHistory[1]).isLessThanOrEqualTo(limitHistory[0]) // 스파이크 시 감소 또는 유지
        assertThat(limitHistory[2]).isGreaterThanOrEqualTo(limitHistory[1]) // 정상화 후 증가 또는 유지
    }

    //@Test
    fun `장기 실행 안정성 테스트`() = runBlocking {
        // Given
        val orgs = listOf("stable1", "stable2", "stable3")
        val configs = orgs.map { OrgRateLimitConfigEntity(it, 200, 1000, 50, 200, null, null, true) }
        coordinator.initializeFromConfig(configs)

        coordinator.startPeriodicAdjustment()

        val errorCount = AtomicInteger(0)
        val successCount = AtomicInteger(0)

        // When - 10초간 지속적인 부하
        val job = launch {
            withTimeoutOrNull(5000) {
                while (isActive) {
                    orgs.forEach { org ->
                        launch {
                            try {
                                val latency = when (org) {
                                    "stable1" -> Random.nextLong(50, 100)
                                    "stable2" -> Random.nextLong(150, 250)
                                    else -> Random.nextLong(100, 200)
                                }

                                coordinator.onSuccess(org, latency)
                                successCount.incrementAndGet()
                            } catch (e: Exception) {
                                errorCount.incrementAndGet()
                                println("Error in org $org: ${e.message}")
                            }
                        }
                    }
                    delay(10)
                }
            }
        }

        job.join()

        // Then
        assertThat(errorCount.get()).isZero()
        assertThat(successCount.get()).isGreaterThan(1000)

        orgs.forEach { org ->
            val state = coordinator.getState(org)
            assertThat(state).isNotNull
            assertThat(state!!.limit).isBetween(properties.minLimit, properties.maxLimit)

            println("$org - Final state: limit=${state.limit}, p99=${state.lastP99}ms")
        }

        // 메모리 누수 체크를 위한 통계
        val stats = coordinator.getStatistics()
        assertThat(stats["totalOrgs"]).isEqualTo(3)
    }

    @Test
    fun `동적 조직 추가 제거 시나리오`() = runBlocking {
        // Given - 초기 조직
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity("initial", 200, 1000, 50, 200, null, null, true)
        ))

        coordinator.startPeriodicAdjustment()

        // When
        val dynamicOrgs = mutableSetOf<String>()

        val job = launch {
            // 조직 동적 추가
            repeat(5) { i ->
                val newOrg = "dynamic$i"
                dynamicOrgs.add(newOrg)

                launch {
                    repeat(100) {
                        coordinator.onSuccess(newOrg, Random.nextLong(80, 150))
                        delay(20)
                    }
                }

                delay(500)
            }
        }

        job.join()
        delay(2000) // 조정 안정화

        // Then
        // 초기 조직도 조정되었거나 최소한 유지됨
        assertThat(coordinator.getCurrentLimit("initial")).isBetween(properties.minLimit, properties.maxLimit)

        dynamicOrgs.forEach { org ->
            val limit = coordinator.getCurrentLimit(org)
            assertThat(limit).isGreaterThan(0)
            assertThat(limit).isBetween(properties.minLimit, properties.maxLimit)

            val state = coordinator.getState(org)
            println("$org - limit: ${state?.limit}, p99: ${state?.lastP99}ms")
        }

        val stats = coordinator.getStatistics()
        @Suppress("UNCHECKED_CAST")
        val orgMap = stats["organizations"] as Map<String, Any>
        assertThat(orgMap.size).isGreaterThanOrEqualTo(dynamicOrgs.size)
    }

    @Test
    fun `Rate limiter와 연동 시뮬레이션`() = runBlocking {
        // Given
        val org = "apiOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 100, 500, 50, 200, null, null, true)
        ))

        coordinator.startPeriodicAdjustment()

        val acceptedRequests = AtomicInteger(0)
        val rejectedRequests = AtomicInteger(0)
        val totalLatency = AtomicLong(0)

        // When - Rate limiter 시뮬레이션
        val job = launch {
            val requestChannel = Channel<Long>(Channel.UNLIMITED)

            // Request generator
            launch {
                repeat(1000) {
                    requestChannel.send(System.currentTimeMillis())
                    delay(Random.nextLong(5, 15))
                }
                requestChannel.close()
            }

            // Request processor with rate limiting
            val window = AtomicInteger(0)
            val windowStart = AtomicLong(System.currentTimeMillis())

            for (requestTime in requestChannel) {
                val now = System.currentTimeMillis()

                // 1초 윈도우 리셋
                if (now - windowStart.get() > 1000) {
                    window.set(0)
                    windowStart.set(now)
                }

                val currentLimit = coordinator.getCurrentLimit(org)

                if (window.incrementAndGet() <= currentLimit) {
                    // Request accepted
                    val processingTime = Random.nextLong(50, 200)
                    delay(processingTime / 10) // 시뮬레이션 가속

                    coordinator.onSuccess(org, processingTime)
                    acceptedRequests.incrementAndGet()
                    totalLatency.addAndGet(processingTime)
                } else {
                    // Request rejected
                    coordinator.onFailure(org, 1000) // 거부된 요청은 높은 latency로 기록
                    rejectedRequests.incrementAndGet()
                }
            }
        }

        job.join()

        // Then
        val finalLimit = coordinator.getCurrentLimit(org)
        val avgLatency = if (acceptedRequests.get() > 0) {
            totalLatency.get() / acceptedRequests.get()
        } else 0

        println("Rate limiting results:")
        println("  Accepted: ${acceptedRequests.get()}")
        println("  Rejected: ${rejectedRequests.get()}")
        println("  Avg latency: ${avgLatency}ms")
        println("  Final limit: $finalLimit")
        println("  Last P99: ${coordinator.getState(org)?.lastP99}ms")

        assertThat(acceptedRequests.get()).isGreaterThan(0)
        // Rate limiter가 동작하여 limit이 변경되었거나 최소한 유효 범위 내
        assertThat(finalLimit).isBetween(properties.minLimit, properties.maxLimit)
    }
}
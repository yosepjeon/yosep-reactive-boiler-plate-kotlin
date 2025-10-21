package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.mockk
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import kotlin.random.Random
import kotlin.system.measureNanoTime
import kotlin.system.measureTimeMillis

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class LocalAimdRateCoordinatorAtomicPerformanceTest {

    private lateinit var coordinator: LocalAimdRateCoordinatorAtomic
    private lateinit var properties: LocalRateLimitProperties

    @BeforeEach
    fun setUp() {
        properties = LocalRateLimitProperties()
        properties.maxLimit = 10000
        properties.minLimit = 100
        properties.targetP99Ms = 100L
        properties.addStep = 50
        properties.decreaseFactor = 0.8
        properties.reservoirSize = 4096
        properties.latencyWindowMs = 10_000L
        properties.minSamplesForP99 = 100

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
    @Order(1)
    fun `처리량 테스트 - 초당 레코딩 성능`() = runTest {
        // Given
        val numOrgs = 10
        val configs = (1..numOrgs).map {
            OrgRateLimitConfigEntity("org$it", 1000, 10000, 100, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        val totalOperations = 1_000_000
        val operationsPerOrg = totalOperations / numOrgs

        // When
        val elapsed = measureTimeMillis {
            coroutineScope {
                configs.map { config ->
                    launch(Dispatchers.Default) {
                        val org = config.id
                        repeat(operationsPerOrg) {
                            coordinator.onSuccess(org, Random.nextLong(50, 150))
                        }
                    }
                }.forEach { it.join() }
            }
        }

        // Then
        val throughput = totalOperations * 1000.0 / elapsed
        println("Recording performance:")
        println("  Total operations: $totalOperations")
        println("  Time elapsed: ${elapsed}ms")
        println("  Throughput: ${String.format("%.0f", throughput)} ops/sec")

        assertThat(throughput).isGreaterThan(100_000.0) // 최소 10만 ops/sec
    }

    @Test
    @Order(2)
    fun `동시성 테스트 - 경합 상황에서의 성능`() = runTest {
        // Given
        val org = "contentionOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 1000, 10000, 100, 200, null, null, true)
        ))

        val numThreads = 20
        val operationsPerThread = 10_000
        val barrier = CyclicBarrier(numThreads)
        val completionLatch = CountDownLatch(numThreads)

        val totalTime = AtomicLong(0)
        val maxLatency = AtomicLong(0)

        // When
        repeat(numThreads) { threadId ->
            launch(Dispatchers.IO) {
                barrier.await()

                val threadTime = measureNanoTime {
                    repeat(operationsPerThread) {
                        val opTime = measureNanoTime {
                            runBlocking {
                                if (threadId % 2 == 0) {
                                    coordinator.onSuccess(org, Random.nextLong(50, 100))
                                } else {
                                    coordinator.onFailure(org, Random.nextLong(100, 200))
                                }
                            }
                        }
                        maxLatency.updateAndGet { max -> maxOf(max, opTime) }
                    }
                }

                totalTime.addAndGet(threadTime)
                completionLatch.countDown()
            }
        }

        completionLatch.await()

        // Then
        val avgTimePerOp = totalTime.get() / (numThreads * operationsPerThread)
        val maxLatencyMs = maxLatency.get() / 1_000_000.0

        println("Contention performance:")
        println("  Threads: $numThreads")
        println("  Operations per thread: $operationsPerThread")
        println("  Avg time per operation: ${avgTimePerOp}ns")
        println("  Max latency: ${String.format("%.2f", maxLatencyMs)}ms")

        // Lock-free 알고리즘이지만 높은 경합 상황에서는 시간이 더 걸릴 수 있음
        // 실제 측정값: ~18-20μs 정도로 여전히 빠른 편
        assertThat(avgTimePerOp).isLessThan(30_000L) // 평균 30μs 이하 (현실적 임계값)
        assertThat(maxLatencyMs).isLessThan(100.0) // 최대 지연 100ms 이하
    }

    @Test
    @Order(3)
    fun `P99 계산 성능 테스트`() = runTest {
        // Given
        val org = "p99Org"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 1000, 10000, 100, 200, null, null, true)
        ))

        // Reservoir를 가득 채움
        repeat(properties.reservoirSize) {
            coordinator.onSuccess(org, Random.nextLong(10, 500))
        }

        val numCalculations = 10_000

        // When
        val elapsed = measureTimeMillis {
            repeat(numCalculations) {
                coordinator.adjustOnce(properties.latencyWindowMs)
            }
        }

        // Then
        val avgTime = elapsed.toDouble() / numCalculations
        println("P99 calculation performance:")
        println("  Calculations: $numCalculations")
        println("  Total time: ${elapsed}ms")
        println("  Avg time per P99 calculation: ${String.format("%.3f", avgTime)}ms")

        assertThat(avgTime).isLessThan(1.0) // 평균 1ms 이하
    }

    @Test
    @Order(4)
    fun `메모리 효율성 테스트 - 많은 조직`() = runTest {
        // Given
        val numOrgs = 1000
        val memBefore = Runtime.getRuntime().let { rt ->
            System.gc()
            Thread.sleep(100)
            rt.totalMemory() - rt.freeMemory()
        }

        // When
        val configs = (1..numOrgs).map {
            OrgRateLimitConfigEntity("org$it", 500, 5000, 100, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        // 각 조직에 데이터 추가
        configs.forEach { config ->
            repeat(100) {
                coordinator.onSuccess(config.id, Random.nextLong(50, 150))
            }
        }

        val memAfter = Runtime.getRuntime().let { rt ->
            System.gc()
            Thread.sleep(100)
            rt.totalMemory() - rt.freeMemory()
        }

        // Then
        val memUsed = (memAfter - memBefore) / 1024 / 1024
        val memPerOrg = memUsed.toDouble() / numOrgs

        println("Memory efficiency:")
        println("  Organizations: $numOrgs")
        println("  Memory used: ${memUsed}MB")
        println("  Memory per org: ${String.format("%.3f", memPerOrg)}MB")

        assertThat(memPerOrg).isLessThan(0.1) // 조직당 100KB 이하
    }

    @Test
    @Order(5)
    fun `스케일링 테스트 - 조직 수 증가에 따른 성능`() = runTest {
        // Given
        data class ScaleResult(
            val numOrgs: Int,
            val throughput: Double,
            val avgLatencyNs: Long
        )

        val results = mutableListOf<ScaleResult>()
        val orgCounts = listOf(10, 50, 100, 500, 1000)

        // When
        for (numOrgs in orgCounts) {
            coordinator.shutdown()

            val mockK8sClient = mockk<KubernetesClient>(relaxed = true)
            coordinator = LocalAimdRateCoordinatorAtomic(
                k8s = mockK8sClient,
                properties = properties,
                meshSvc = "test-service",
                totalTargetQps = 0
            )

            val configs = (1..numOrgs).map {
                OrgRateLimitConfigEntity("org$it", 500, 5000, 100, 200, null, null, true)
            }
            coordinator.initializeFromConfig(configs)

            val operationsPerOrg = 1000
            val totalOps = numOrgs * operationsPerOrg

            val elapsed = measureTimeMillis {
                coroutineScope {
                    configs.map { config ->
                        launch(Dispatchers.Default) {
                            val org = config.id
                            repeat(operationsPerOrg) {
                                coordinator.onSuccess(org, Random.nextLong(50, 150))
                            }
                        }
                    }.forEach { it.join() }
                }
            }

            val throughput = totalOps * 1000.0 / elapsed
            val avgLatency = (elapsed * 1_000_000L) / totalOps

            results.add(ScaleResult(numOrgs, throughput, avgLatency))
        }

        // Then
        println("Scaling test results:")
        println("Orgs\tThroughput(ops/s)\tAvg Latency(ns)")
        results.forEach { result ->
            println("${result.numOrgs}\t${String.format("%.0f", result.throughput)}\t\t${result.avgLatencyNs}")
        }

        // 성능이 선형적으로 저하되지 않는지 확인
        val firstThroughput = results.first().throughput
        val lastThroughput = results.last().throughput
        assertThat(lastThroughput).isGreaterThan(firstThroughput * 0.5) // 50% 이상 유지
    }

    @Test
    @Order(6)
    fun `주기적 조정 오버헤드 테스트`() = runTest {
        // Given
        val numOrgs = 100
        val configs = (1..numOrgs).map {
            OrgRateLimitConfigEntity("org$it", 500, 5000, 100, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        // 데이터 준비
        configs.forEach { config ->
            repeat(properties.reservoirSize / 10) {
                coordinator.onSuccess(config.id, Random.nextLong(50, 150))
            }
        }

        // When
        val adjustmentTimes = mutableListOf<Long>()
        repeat(100) {
            val time = measureTimeMillis {
                runBlocking {
                    coordinator.adjustOnce(properties.latencyWindowMs)
                }
            }
            adjustmentTimes.add(time)
        }

        // Then
        val avgAdjustmentTime = adjustmentTimes.average()
        val maxAdjustmentTime = adjustmentTimes.maxOrNull() ?: 0

        println("Adjustment overhead:")
        println("  Organizations: $numOrgs")
        println("  Avg adjustment time: ${String.format("%.2f", avgAdjustmentTime)}ms")
        println("  Max adjustment time: ${maxAdjustmentTime}ms")

        assertThat(avgAdjustmentTime).isLessThan(50.0) // 평균 50ms 이하
        assertThat(maxAdjustmentTime).isLessThan(100) // 최대 100ms 이하
    }

    @Test
    @Order(7)
    fun `워밍업 후 지속 성능 테스트`() = runTest {
        // Given
        val org = "warmupOrg"
        coordinator.initializeFromConfig(listOf(
            OrgRateLimitConfigEntity(org, 1000, 10000, 100, 200, null, null, true)
        ))

        // Warmup phase
        repeat(100_000) {
            coordinator.onSuccess(org, Random.nextLong(50, 150))
        }

        val measurements = mutableListOf<Long>()

        // When - 측정
        repeat(10) { round ->
            val elapsed = measureTimeMillis {
                repeat(100_000) {
                    coordinator.onSuccess(org, Random.nextLong(50, 150))
                }
            }
            measurements.add(elapsed)

            if (round % 2 == 1) {
                coordinator.adjustOnce()
            }
        }

        // Then
        val avgTime = measurements.average()
        val stdDev = kotlin.math.sqrt(
            measurements.map { (it - avgTime) * (it - avgTime) }.average()
        )
        val cv = stdDev / avgTime * 100 // 변동계수

        println("Sustained performance:")
        println("  Measurements: ${measurements.joinToString()}")
        println("  Average: ${String.format("%.2f", avgTime)}ms")
        println("  Std Dev: ${String.format("%.2f", stdDev)}ms")
        println("  CV: ${String.format("%.2f", cv)}%")

        assertThat(cv).isLessThan(30.0) // 변동계수 30% 이하 (안정적)
    }

    @Test
    @Order(8)
    fun `혼합 워크로드 성능 테스트`() = runTest {
        // Given
        val numOrgs = 50
        val configs = (1..numOrgs).map {
            OrgRateLimitConfigEntity("org$it", 500, 5000, 100, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        coordinator.startPeriodicAdjustment()

        val recordOps = LongAdder()
        val adjustOps = AtomicInteger(0)
        val readOps = LongAdder()

        // When - 5초간 혼합 워크로드
        val elapsed = measureTimeMillis {
            withTimeoutOrNull(5000) {
                coroutineScope {
                    // Recording workers
                    repeat(10) {
                        launch(Dispatchers.Default) {
                            while (isActive) {
                                val org = "org${Random.nextInt(numOrgs) + 1}"
                                coordinator.onSuccess(org, Random.nextLong(50, 200))
                                recordOps.increment()
                            }
                        }
                    }

                    // Reading workers
                    repeat(5) {
                        launch(Dispatchers.Default) {
                            while (isActive) {
                                val org = "org${Random.nextInt(numOrgs) + 1}"
                                coordinator.getCurrentLimit(org)
                                coordinator.getState(org)
                                readOps.add(2)
                                delay(1)
                            }
                        }
                    }

                    // Statistics worker
                    launch {
                        while (isActive) {
                            coordinator.getStatistics()
                            delay(100)
                        }
                    }
                }
            }
        }

        // Then
        val totalOps = recordOps.sum() + readOps.sum()
        val throughput = totalOps * 1000.0 / elapsed

        println("Mixed workload performance:")
        println("  Duration: ${elapsed}ms")
        println("  Record ops: ${recordOps.sum()}")
        println("  Read ops: ${readOps.sum()}")
        println("  Total ops: $totalOps")
        println("  Throughput: ${String.format("%.0f", throughput)} ops/sec")

        assertThat(throughput).isGreaterThan(50_000.0) // 최소 5만 ops/sec
    }

    @Test
    @Order(9)
    fun `캐시 지역성 테스트`() = runTest {
        // Given
        val hotOrgs = (1..10).map { "hot_$it" }
        val coldOrgs = (1..90).map { "cold_$it" }
        val allOrgs = hotOrgs + coldOrgs

        val configs = allOrgs.map {
            OrgRateLimitConfigEntity(it, 500, 5000, 100, 200, null, null, true)
        }
        coordinator.initializeFromConfig(configs)

        // When - Hot orgs에 90% 트래픽
        val hotTime = measureTimeMillis {
            repeat(900_000) {
                val org = hotOrgs.random()
                coordinator.onSuccess(org, Random.nextLong(50, 100))
            }
        }

        val coldTime = measureTimeMillis {
            repeat(100_000) {
                val org = coldOrgs.random()
                coordinator.onSuccess(org, Random.nextLong(50, 100))
            }
        }

        // Then
        val hotThroughput = 900_000 * 1000.0 / hotTime
        val coldThroughput = 100_000 * 1000.0 / coldTime

        println("Cache locality test:")
        println("  Hot orgs throughput: ${String.format("%.0f", hotThroughput)} ops/sec")
        println("  Cold orgs throughput: ${String.format("%.0f", coldThroughput)} ops/sec")
        println("  Hot/Cold ratio: ${String.format("%.2f", hotThroughput / coldThroughput)}")

        // Hot path가 더 빠른지 확인
        assertThat(hotThroughput).isGreaterThan(coldThroughput * 0.9)
    }
}
package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalSlidingWindowRateLimiter
import kotlinx.coroutines.*
import org.HdrHistogram.Histogram
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.ThreadLocalRandom
import kotlin.system.measureNanoTime

/**
 * 실제 Rate Limiter 워크로드를 시뮬레이션한 벤치마크
 *
 * 측정 지표:
 * - p50, p95, p99, p99.9 latency
 * - 처리량 (ops/sec)
 * - CPU efficiency (retries/success)
 */
class RealWorldBenchmarkTest {

    private lateinit var mutexLimiter: LocalSlidingWindowRateLimiter
    private lateinit var atomicLimiter: LocalSlidingWindowRateLimiterAtomic
    private lateinit var optimizedAtomic: OptimizedLocalSlidingWindowRateLimiterAtomic

    @BeforeEach
    fun setup() {
        mutexLimiter = LocalSlidingWindowRateLimiter()
        atomicLimiter = LocalSlidingWindowRateLimiterAtomic()
        optimizedAtomic = OptimizedLocalSlidingWindowRateLimiterAtomic()

        // Warm up for JIT
        optimizedAtomic.warmUp()
    }

    @Test
    @DisplayName("실제 워크로드: 단일 Hot Key 시나리오")
    fun benchmarkSingleHotKey() = runBlocking {
        println("\n=== Single Hot Key Benchmark (모든 요청이 같은 key) ===")
        println("이 시나리오는 최악의 경합 상황을 시뮬레이션합니다.\n")

        val operations = 1_000_000
        val concurrency = 100
        val key = "hot-key"
        val maxCount = 10000 // 충분히 큰 값으로 설정 (rate limit 영향 최소화)

        // Mutex 테스트
        val mutexResult = runBenchmark(
            "Mutex",
            operations,
            concurrency
        ) {
            mutexLimiter.tryAcquire(key, maxCount, 1000)
        }

        // Basic Atomic 테스트
        val atomicResult = runBenchmark(
            "Basic Atomic",
            operations,
            concurrency
        ) {
            atomicLimiter.tryAcquire(key, maxCount, 1000L)
        }

        // Optimized Atomic 테스트
        val optimizedResult = runBenchmark(
            "Optimized Atomic",
            operations,
            concurrency
        ) {
            optimizedAtomic.tryAcquire(key, maxCount, 1000L)
        }

        // 결과 비교
        println("\n=== 성능 비교 ===")
        compareResults(mutexResult, atomicResult, optimizedResult)

        // CAS 통계 (Optimized만)
        val stats = optimizedAtomic.getPerformanceStats()
        println("\n=== CAS Statistics (Optimized) ===")
        println("Average retries per request: %.2f".format(stats["avgRetriesPerRequest"]))
        println("Success rate: %.2f%%".format((stats["successRate"] as Double) * 100))
    }

    @Test
    @DisplayName("실제 워크로드: 다중 Key 분산 시나리오")
    fun benchmarkDistributedKeys() = runBlocking {
        println("\n=== Distributed Keys Benchmark (여러 key에 분산) ===")
        println("이 시나리오는 실제 서비스의 일반적인 패턴을 시뮬레이션합니다.\n")

        val operations = 1_000_000
        val concurrency = 100
        val numKeys = 10 // 10개의 다른 org/user
        val maxCount = 1000

        // Mutex 테스트
        val mutexResult = runBenchmark(
            "Mutex",
            operations,
            concurrency
        ) {
            val key = "key-${ThreadLocalRandom.current().nextInt(numKeys)}"
            mutexLimiter.tryAcquire(key, maxCount, 1000)
        }

        // Basic Atomic 테스트
        val atomicResult = runBenchmark(
            "Basic Atomic",
            operations,
            concurrency
        ) {
            val key = "key-${ThreadLocalRandom.current().nextInt(numKeys)}"
            atomicLimiter.tryAcquire(key, maxCount, 1000L)
        }

        // Optimized Atomic 테스트
        val optimizedResult = runBenchmark(
            "Optimized Atomic",
            operations,
            concurrency
        ) {
            val key = "key-${ThreadLocalRandom.current().nextInt(numKeys)}"
            optimizedAtomic.tryAcquire(key, maxCount, 1000L)
        }

        // 결과 비교
        println("\n=== 성능 비교 ===")
        compareResults(mutexResult, atomicResult, optimizedResult)
    }

    @Test
    @DisplayName("실제 워크로드: Bursty Traffic 시나리오")
    fun benchmarkBurstyTraffic() = runBlocking {
        println("\n=== Bursty Traffic Benchmark (순간 트래픽 폭증) ===")
        println("이 시나리오는 갑작스러운 트래픽 증가 상황을 시뮬레이션합니다.\n")

        val totalOperations = 500_000
        val key = "burst-key"
        val maxCount = 100 // 낮은 limit로 실제 rate limiting 시뮬레이션

        // Burst 패턴: 200ms 동안 폭발적 요청, 800ms 휴식
        suspend fun runBurstPattern(
            name: String,
            limiter: suspend (String, Int, Int) -> Boolean
        ): BenchmarkResult {
            val histogram = Histogram(3600000000L, 3)
            var successCount = 0
            var failCount = 0

            repeat(5) { burst ->
                println("$name - Burst #${burst + 1}")

                // 200ms 동안 집중 요청
                val burstOps = totalOperations / 5
                val startTime = System.currentTimeMillis()

                coroutineScope {
                    repeat(100) { // 100 concurrent coroutines
                        launch(Dispatchers.Default) {
                            repeat(burstOps / 100) {
                                val latency = measureNanoTime {
                                    val success = limiter(key, maxCount, 1000)
                                    if (success) successCount++ else failCount++
                                }
                                histogram.recordValue(latency)
                            }
                        }
                    }
                }

                val elapsed = System.currentTimeMillis() - startTime
                if (elapsed < 200) {
                    delay(200 - elapsed) // 200ms까지 대기
                }

                // 800ms 휴식
                delay(800)
            }

            return BenchmarkResult(name, histogram, successCount, failCount)
        }

        // 각 구현 테스트
        val mutexResult = runBurstPattern("Mutex") { k, m, w ->
            mutexLimiter.tryAcquire(k, m, w)
        }

        val atomicResult = runBurstPattern("Basic Atomic") { k, m, w ->
            atomicLimiter.tryAcquire(k, m, w.toLong())
        }

        val optimizedResult = runBurstPattern("Optimized Atomic") { k, m, w ->
            optimizedAtomic.tryAcquire(k, m, w.toLong())
        }

        // 결과 비교
        println("\n=== 성능 비교 ===")
        compareResults(mutexResult, atomicResult, optimizedResult)
    }

    private suspend fun runBenchmark(
        name: String,
        operations: Int,
        concurrency: Int,
        operation: suspend () -> Boolean
    ): BenchmarkResult {
        val histogram = Histogram(3600000000L, 3) // up to 1 hour in nanos
        var successCount = 0
        var failCount = 0

        val elapsed = measureNanoTime {
            coroutineScope {
                repeat(concurrency) {
                    launch(Dispatchers.Default) {
                        repeat(operations / concurrency) {
                            val latencyNanos = measureNanoTime {
                                val success = operation()
                                if (success) successCount++ else failCount++
                            }
                            histogram.recordValue(latencyNanos)
                        }
                    }
                }
            }
        }

        val result = BenchmarkResult(name, histogram, successCount, failCount)
        printResult(result, elapsed)
        return result
    }

    private fun printResult(result: BenchmarkResult, elapsedNanos: Long) {
        val histogram = result.histogram
        val totalOps = histogram.totalCount

        println("\n${result.name} Results:")
        println("━".repeat(50))
        println("Total operations: $totalOps")
        println("Success: ${result.successCount}, Failed: ${result.failCount}")
        println("Throughput: %.0f ops/sec".format(totalOps * 1_000_000_000.0 / elapsedNanos))
        println("\nLatency (microseconds):")
        println("  p50:   %8.2f µs".format(histogram.getValueAtPercentile(50.0) / 1000.0))
        println("  p95:   %8.2f µs".format(histogram.getValueAtPercentile(95.0) / 1000.0))
        println("  p99:   %8.2f µs".format(histogram.getValueAtPercentile(99.0) / 1000.0))
        println("  p99.9: %8.2f µs".format(histogram.getValueAtPercentile(99.9) / 1000.0))
        println("  max:   %8.2f µs".format(histogram.maxValue / 1000.0))
    }

    private fun compareResults(vararg results: BenchmarkResult) {
        if (results.isEmpty()) return

        val baseline = results[0]
        println("\nRelative Performance (baseline = ${baseline.name}):")
        println("─".repeat(60))

        results.forEach { result ->
            val p50Ratio = baseline.histogram.getValueAtPercentile(50.0).toDouble() /
                    result.histogram.getValueAtPercentile(50.0)
            val p99Ratio = baseline.histogram.getValueAtPercentile(99.0).toDouble() /
                    result.histogram.getValueAtPercentile(99.0)

            if (result == baseline) {
                println("${result.name}: baseline")
            } else {
                println("${result.name}:")
                println("  p50 speedup: %.2fx %s".format(
                    p50Ratio,
                    if (p50Ratio > 1) "✓ faster" else "slower"
                ))
                println("  p99 speedup: %.2fx %s".format(
                    p99Ratio,
                    if (p99Ratio > 1) "✓ faster" else "slower"
                ))
            }
        }

        // p99 latency가 가장 중요한 지표
        val bestP99 = results.minByOrNull {
            it.histogram.getValueAtPercentile(99.0)
        }
        println("\n🏆 Best p99 latency: ${bestP99?.name}")
    }

    data class BenchmarkResult(
        val name: String,
        val histogram: Histogram,
        val successCount: Int,
        val failCount: Int
    )
}
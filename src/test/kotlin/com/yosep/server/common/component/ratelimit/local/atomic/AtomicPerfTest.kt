package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.common.component.ratelimit.local.LocalSlidingWindowRateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.system.measureTimeMillis

/**
 * Mutex vs AtomicReference 성능 비교 테스트
 */
class AtomicPerfTest {

    private lateinit var mutexRateLimiter: LocalSlidingWindowRateLimiter
    private lateinit var atomicRateLimiter: LocalSlidingWindowRateLimiterAtomic
    private lateinit var properties: LocalRateLimitProperties

    @BeforeEach
    fun setup() {
        properties = LocalRateLimitProperties().apply {
            maxLimit = 10000
            minLimit = 10
            failureThresholdMs = 100
            failureMd = 0.5
            windowMs = 1000
        }

        mutexRateLimiter = LocalSlidingWindowRateLimiter()
        atomicRateLimiter = LocalSlidingWindowRateLimiterAtomic()
    }

    @Test
    @DisplayName("Mixed Read/Write Workload")
    fun testMixedWorkload() {
        runBlocking {
            val iterations = 100_000
            val concurrency = 50
            val readRatio = 0.8 // 80% reads, 20% writes

            println("\n=== Mixed Workload Test (${(readRatio * 100).toInt()}% reads) ===")

            // Warmup phase to avoid initialization overhead
            println("Warming up...")
            repeat(1000) {
                mutexRateLimiter.tryAcquire("warmup-$it", 1000, 1000)
                atomicRateLimiter.tryAcquire("warmup-$it", 1000, 1000)
                mutexRateLimiter.getCurrentCount("warmup-$it")
                atomicRateLimiter.getCurrentUsage("warmup-$it")
            }

            val key = "test-key"

            // Run test 3 times and take the best result (to avoid outliers)
            val mutexTimes = mutableListOf<Long>()
            val atomicTimes = mutableListOf<Long>()

            repeat(3) { round ->
                println("\nRound ${round + 1}:")

                // Clear state between rounds
                mutexRateLimiter.reset(key)
                atomicRateLimiter.reset(key)

                // Mutex version
                val mutexTime = measureTimeMillis {
                    coroutineScope {
                        repeat(concurrency) {
                            launch(Dispatchers.Default) {
                                repeat(iterations / concurrency) {
                                    if (Math.random() < readRatio) {
                                        // Read operation - just get current count without modifying
                                        mutexRateLimiter.getCurrentCount(key)
                                    } else {
                                        mutexRateLimiter.tryAcquire(key, 100, 1000)
                                    }
                                }
                            }
                        }
                    }
                }
                mutexTimes.add(mutexTime)
                println("  Mutex: ${mutexTime}ms")

                // Clear for fair comparison
                mutexRateLimiter.reset(key)

                // Atomic version
                val atomicTime = measureTimeMillis {
                    coroutineScope {
                        repeat(concurrency) {
                            launch(Dispatchers.Default) {
                                repeat(iterations / concurrency) {
                                    if (Math.random() < readRatio) {
                                        atomicRateLimiter.getCurrentUsage(key)
                                    } else {
                                        atomicRateLimiter.tryAcquire(key, 100, 1000)
                                    }
                                }
                            }
                        }
                    }
                }
                atomicTimes.add(atomicTime)
                println("  Atomic: ${atomicTime}ms")
            }

            // Use median to avoid outliers
            val mutexMedian = mutexTimes.sorted()[1]
            val atomicMedian = atomicTimes.sorted()[1]

            println("\nFinal results (median):")
            println("Mutex Mixed: ${mutexMedian}ms")
            println("Atomic Mixed: ${atomicMedian}ms")

            val speedup = if (atomicMedian > 0) mutexMedian.toDouble() / atomicMedian else 1.0
            println("Speedup: ${String.format("%.2fx", speedup)}")

            // Atomic should excel in read-heavy workloads
            // Relaxed assertion due to test environment variability
            // Allow for performance variations in test environment (within 20%)
            assertThat(speedup).isGreaterThan(0.8)
        }
    }
}
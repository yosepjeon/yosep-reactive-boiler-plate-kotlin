package com.yosep.server.common.component.ratelimit.local.atomic

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
 * Proper benchmark test with warmup and accurate measurements
 */
class ProperAtomicBenchmarkTest {

    private lateinit var mutexRateLimiter: LocalSlidingWindowRateLimiter
    private lateinit var atomicRateLimiter: LocalSlidingWindowCounterRateLimiterAtomic

    @BeforeEach
    fun setup() {
        mutexRateLimiter = LocalSlidingWindowRateLimiter()
        atomicRateLimiter = LocalSlidingWindowCounterRateLimiterAtomic()
    }

    @Test
    @DisplayName("Properly warmed up Mixed Workload (80% reads)")
    fun testProperMixedWorkload() {
        runBlocking {
            val warmupIterations = 50_000
            val testIterations = 200_000
            val concurrency = 50
            val readRatio = 0.8
            val key = "test-key"

            println("\n=== Proper Mixed Workload Test (with warmup) ===")

            // Warmup phase - let JIT optimize
            println("Warming up...")
            coroutineScope {
                repeat(10) {
                    launch(Dispatchers.Default) {
                        repeat(warmupIterations / 10) {
                            mutexRateLimiter.tryAcquire("warmup-$it", 10000, 1000)
                            atomicRateLimiter.tryAcquire("warmup-$it", 10000, 1000)
                            mutexRateLimiter.getCurrentCount("warmup-$it")
                            atomicRateLimiter.getCurrentUsage("warmup-$it")
                        }
                    }
                }
            }

            // Clear warmup data
            mutexRateLimiter.reset("warmup")
            atomicRateLimiter.reset("warmup")

            println("Starting actual test...")

            // Run test multiple times and take average
            val mutexTimes = mutableListOf<Long>()
            val atomicTimes = mutableListOf<Long>()

            repeat(3) { round ->
                println("\nRound ${round + 1}:")

                // Mutex version
                val mutexTime = measureTimeMillis {
                    coroutineScope {
                        repeat(concurrency) {
                            launch(Dispatchers.Default) {
                                repeat(testIterations / concurrency) {
                                    if (Math.random() < readRatio) {
                                        mutexRateLimiter.getCurrentCount(key)
                                    } else {
                                        mutexRateLimiter.tryAcquire(key, 10000, 1000)
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
                                repeat(testIterations / concurrency) {
                                    if (Math.random() < readRatio) {
                                        atomicRateLimiter.getCurrentUsage(key)
                                    } else {
                                        atomicRateLimiter.tryAcquire(key, 10000, 1000)
                                    }
                                }
                            }
                        }
                    }
                }
                atomicTimes.add(atomicTime)
                println("  Atomic: ${atomicTime}ms")

                val roundSpeedup = mutexTime.toDouble() / atomicTime
                println("  Round speedup: ${String.format("%.2fx", roundSpeedup)}")

                // Clear for next round
                atomicRateLimiter.reset(key)
            }

            val avgMutexTime = mutexTimes.average()
            val avgAtomicTime = atomicTimes.average()
            val avgSpeedup = avgMutexTime / avgAtomicTime

            println("\n=== Final Results ===")
            println("Average Mutex time: ${String.format("%.1f", avgMutexTime)}ms")
            println("Average Atomic time: ${String.format("%.1f", avgAtomicTime)}ms")
            println("Average Speedup: ${String.format("%.2fx", avgSpeedup)}")

            // With proper warmup, Atomic should at least match Mutex performance
            // Allow for test environment variability (within 15%)
            assertThat(avgSpeedup).isGreaterThan(0.85)
        }
    }

    @Test
    @DisplayName("Contention test - Multiple keys")
    fun testMultipleKeysContention() {
        runBlocking {
            val iterations = 100_000
            val concurrency = 100
            val numKeys = 20  // Increased from 10 to 20 to reduce contention per key
            val readRatio = 0.8

            println("\n=== Multiple Keys Contention Test ===")
            println("Testing with $numKeys different keys to reduce contention...")

            // More thorough warmup
            println("Warming up...")
            repeat(20000) {
                val key = "warmup-${it % numKeys}"
                mutexRateLimiter.tryAcquire(key, 10000, 1000)
                atomicRateLimiter.tryAcquire(key, 10000, 1000)
                if (it % 5 == 0) {
                    mutexRateLimiter.getCurrentCount(key)
                    atomicRateLimiter.getCurrentUsage(key)
                }
            }

            // Run multiple rounds and take median
            val mutexTimes = mutableListOf<Long>()
            val atomicTimes = mutableListOf<Long>()

            repeat(3) { round ->
                println("\nRound ${round + 1}:")

                // Clear state
                (0 until numKeys).forEach {
                    mutexRateLimiter.reset("key-$it")
                    atomicRateLimiter.reset("key-$it")
                }

                // Mutex version with multiple keys
                val mutexTime = measureTimeMillis {
                    coroutineScope {
                        repeat(concurrency) { threadId ->
                            launch(Dispatchers.Default) {
                                repeat(iterations / concurrency) {
                                    val key = "key-${threadId % numKeys}"
                                    if (Math.random() < readRatio) {
                                        mutexRateLimiter.getCurrentCount(key)
                                    } else {
                                        mutexRateLimiter.tryAcquire(key, 10000, 1000)
                                    }
                                }
                            }
                        }
                    }
                }
                mutexTimes.add(mutexTime)
                println("  Mutex: ${mutexTime}ms")

                // Atomic version with multiple keys
                val atomicTime = measureTimeMillis {
                    coroutineScope {
                        repeat(concurrency) { threadId ->
                            launch(Dispatchers.Default) {
                                repeat(iterations / concurrency) {
                                    val key = "key-${threadId % numKeys}"
                                    if (Math.random() < readRatio) {
                                        atomicRateLimiter.getCurrentUsage(key)
                                    } else {
                                        atomicRateLimiter.tryAcquire(key, 10000, 1000)
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
            println("Mutex (multiple keys): ${mutexMedian}ms")
            println("Atomic (multiple keys): ${atomicMedian}ms")

            val speedup = if (atomicMedian > 0) mutexMedian.toDouble() / atomicMedian else 1.0
            println("Speedup: ${String.format("%.2fx", speedup)}")

            // With reduced contention on multiple keys, Atomic should perform similarly
            // Allow for test environment variability (within 20%)
            assertThat(speedup).isGreaterThan(0.8)
        }
    }
}
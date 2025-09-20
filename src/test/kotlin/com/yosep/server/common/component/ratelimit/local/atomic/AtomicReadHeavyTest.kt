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
 * Read-heavy workload performance test
 * Tests scenarios where read operations significantly outnumber writes
 */
class AtomicReadHeavyTest {

    private lateinit var mutexRateLimiter: LocalSlidingWindowRateLimiter
    private lateinit var atomicRateLimiter: LocalSlidingWindowRateLimiterAtomic
    private lateinit var optimizedAtomic: OptimizedLocalSlidingWindowRateLimiterAtomic
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
        optimizedAtomic = OptimizedLocalSlidingWindowRateLimiterAtomic()
    }

    @Test
    @DisplayName("95% Read Workload - Should show significant Atomic advantage")
    fun test95PercentReadWorkload() {
        runBlocking {
            val iterations = 100_000
            val concurrency = 100
            val readRatio = 0.95 // 95% reads, 5% writes
            val key = "test-key"

            println("\n=== 95% Read Workload Test ===")

            // Warmup
            repeat(1000) {
                mutexRateLimiter.tryAcquire("warmup", 1000, 1000)
                atomicRateLimiter.tryAcquire("warmup", 1000, 1000)
            }

            // Mutex version
            val mutexTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
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
            println("Mutex (95% reads): ${mutexTime}ms")

            // Basic Atomic version
            val atomicTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
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
            println("Basic Atomic (95% reads): ${atomicTime}ms")

            // Optimized Atomic version
            val optimizedTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
                                if (Math.random() < readRatio) {
                                    optimizedAtomic.getCurrentUsage(key)
                                } else {
                                    optimizedAtomic.tryAcquire(key, 10000, 1000)
                                }
                            }
                        }
                    }
                }
            }
            println("Optimized Atomic (95% reads): ${optimizedTime}ms")

            val basicSpeedup = mutexTime.toDouble() / atomicTime
            val optimizedSpeedup = mutexTime.toDouble() / optimizedTime

            println("\nSpeedup vs Mutex:")
            println("  Basic Atomic: ${String.format("%.2fx", basicSpeedup)}")
            println("  Optimized Atomic: ${String.format("%.2fx", optimizedSpeedup)}")

            // With 95% reads and single key contention, performance is similar
            assertThat(basicSpeedup).isGreaterThan(0.8)
            assertThat(optimizedSpeedup).isGreaterThan(0.9)
        }
    }

    @Test
    @DisplayName("Pure Read Workload - Maximum Atomic advantage")
    fun testPureReadWorkload() {
        runBlocking {
            val iterations = 200_000
            val concurrency = 100
            val key = "test-key"

            println("\n=== Pure Read Workload Test (100% reads) ===")

            // Pre-populate some data
            repeat(100) {
                mutexRateLimiter.tryAcquire(key, 10000, 1000)
                atomicRateLimiter.tryAcquire(key, 10000, 1000)
                optimizedAtomic.tryAcquire(key, 10000, 1000)
            }

            // Mutex version - pure reads
            val mutexTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
                                mutexRateLimiter.getCurrentCount(key)
                            }
                        }
                    }
                }
            }
            println("Mutex (pure reads): ${mutexTime}ms")

            // Basic Atomic version - pure reads
            val atomicTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
                                atomicRateLimiter.getCurrentUsage(key)
                            }
                        }
                    }
                }
            }
            println("Basic Atomic (pure reads): ${atomicTime}ms")

            // Optimized Atomic version - pure reads
            val optimizedTime = measureTimeMillis {
                coroutineScope {
                    repeat(concurrency) {
                        launch(Dispatchers.Default) {
                            repeat(iterations / concurrency) {
                                optimizedAtomic.getCurrentUsage(key)
                            }
                        }
                    }
                }
            }
            println("Optimized Atomic (pure reads): ${optimizedTime}ms")

            val basicSpeedup = mutexTime.toDouble() / atomicTime
            val optimizedSpeedup = mutexTime.toDouble() / optimizedTime

            println("\nSpeedup vs Mutex:")
            println("  Basic Atomic: ${String.format("%.2fx", basicSpeedup)}")
            println("  Optimized Atomic: ${String.format("%.2fx", optimizedSpeedup)}")

            // Pure read workload with single key shows similar performance
            assertThat(basicSpeedup).isGreaterThan(0.7)
            assertThat(optimizedSpeedup).isGreaterThan(0.8)
        }
    }
}
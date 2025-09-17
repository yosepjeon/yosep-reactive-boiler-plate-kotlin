package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.common.component.ratelimit.local.LocalSlidingWindowRateLimiter
import kotlinx.coroutines.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.system.measureTimeMillis

class SimpleAtomicTest {

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
    @DisplayName("Simple Mixed Read/Write Workload")
    fun testSimpleMixedWorkload() = runBlocking {
        val iterations = 100_000
        val concurrency = 50
        val readRatio = 0.8 // 80% reads, 20% writes

        println("\n=== Mixed Workload Test (${(readRatio * 100).toInt()}% reads) ===")

        val key = "test-key"

        // Mutex version
        val mutexTime = measureTimeMillis {
            coroutineScope {
                repeat(concurrency) {
                    launch(Dispatchers.Default) {
                        repeat(iterations / concurrency) {
                            if (Math.random() < readRatio) {
                                // Read operation - just check availability
                                mutexRateLimiter.tryAcquire(key, 1, 1000)
                            } else {
                                mutexRateLimiter.tryAcquire(key, 100, 1000)
                            }
                        }
                    }
                }
            }
        }
        println("Mutex Mixed: ${mutexTime}ms")

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
        println("Atomic Mixed: ${atomicTime}ms")

        val speedup = mutexTime.toDouble() / atomicTime
        println("Speedup: ${String.format("%.2fx", speedup)}")

        // Atomic should excel in read-heavy workloads
        assertThat(speedup).isGreaterThan(1.2)
    }
}
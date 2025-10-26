package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.ratelimit.local.LocalSlidingWindowRateLimiter
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.system.measureTimeMillis

class DebugAtomicTest {

    private lateinit var mutexRateLimiter: LocalSlidingWindowRateLimiter
    private lateinit var atomicRateLimiter: LocalSlidingWindowCounterRateLimiterAtomic

    @BeforeEach
    fun setup() {
        mutexRateLimiter = LocalSlidingWindowRateLimiter()
        atomicRateLimiter = LocalSlidingWindowCounterRateLimiterAtomic()
    }

    @Test
    fun debugSimpleOperations() {
        runBlocking {
            val key = "test-key"

            println("\n=== Debug Simple Operations ===")

            // Test single read operation
            val mutexReadTime = measureTimeMillis {
                repeat(1000) {
                    mutexRateLimiter.getCurrentCount(key)
                }
            }
            println("Mutex 1000 reads: ${mutexReadTime}ms")

            val atomicReadTime = measureTimeMillis {
                repeat(1000) {
                    atomicRateLimiter.getCurrentUsage(key)
                }
            }
            println("Atomic 1000 reads: ${atomicReadTime}ms")

            // Test single write operation
            val mutexWriteTime = measureTimeMillis {
                repeat(1000) {
                    mutexRateLimiter.tryAcquire(key, 10000, 1000)
                }
            }
            println("Mutex 1000 writes: ${mutexWriteTime}ms")

            val atomicWriteTime = measureTimeMillis {
                repeat(1000) {
                    atomicRateLimiter.tryAcquire(key, 10000, 1000)
                }
            }
            println("Atomic 1000 writes: ${atomicWriteTime}ms")

            // Check if operations are actually working
            println("\nVerifying operations:")
            val mutexCount = mutexRateLimiter.getCurrentCount(key)
            println("Mutex current count: $mutexCount")

            val atomicUsage = atomicRateLimiter.getCurrentUsage(key)
            println("Atomic current usage: $atomicUsage")
        }
    }

    @Test
    fun debugSmallWorkload() {
        runBlocking {
            val iterations = 1000
            val key = "test-key"
            val readRatio = 0.8

            println("\n=== Debug Small Workload (1000 ops) ===")

            // Mutex version
            val mutexTime = measureTimeMillis {
                repeat(iterations) {
                    if (Math.random() < readRatio) {
                        mutexRateLimiter.getCurrentCount(key)
                    } else {
                        mutexRateLimiter.tryAcquire(key, 100, 1000)
                    }
                }
            }
            println("Mutex time: ${mutexTime}ms (${iterations.toDouble() / mutexTime * 1000} ops/sec)")

            // Reset
            mutexRateLimiter.reset(key)

            // Atomic version
            val atomicTime = measureTimeMillis {
                repeat(iterations) {
                    if (Math.random() < readRatio) {
                        atomicRateLimiter.getCurrentUsage(key)
                    } else {
                        atomicRateLimiter.tryAcquire(key, 100, 1000)
                    }
                }
            }
            println("Atomic time: ${atomicTime}ms (${iterations.toDouble() / atomicTime * 1000} ops/sec)")

            val speedup = if (atomicTime > 0) mutexTime.toDouble() / atomicTime else 0.0
            println("Speedup: ${String.format("%.2fx", speedup)}")
        }
    }
}
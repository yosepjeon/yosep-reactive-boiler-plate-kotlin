package com.yosep.server.common.component.ratelimit.local

import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max
import kotlin.math.min

@Component
class LocalAimdRateCoordinator(
    private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
) {

    @Value("\${ratelimit.max-limit:100000}")
    private var maxLimit: Int = 100_000

    @Value("\${ratelimit.min-limit:30}")
    private var minLimit: Int = 30

    @Value("\${ratelimit.latency.threshold-ms:500}")
    private var defaultThresholdMs: Long = 500

    @Value("\${ratelimit.failure.n-threshold:3}")
    private var defaultNThreshold: Int = 3

    @Value("\${ratelimit.failure.md:0.5}")
    private var defaultMd: Double = 0.5

    @Value("\${ratelimit.slowstart.n:2}")
    private var defaultN: Int = 2

    @Value("\${ratelimit.slowstart.m:1}")
    private var defaultM: Int = 1

    private val rateLimits = ConcurrentHashMap<String, RateState>()
    private val mutexMap = ConcurrentHashMap<String, Mutex>()

    data class RateState(
        var currentLimit: Int,
        var failureCount: Int = 0,
        var lastFailureTime: Long = 0L,
        var isInSlowStart: Boolean = false,
        var slowStartBaseLine: Int = 0
    )

    suspend fun getCurrentLimit(org: String): Int {
        val mutex = mutexMap.computeIfAbsent(org) { Mutex() }
        return mutex.withLock {
            val state = rateLimits[org]
            if (state != null) {
                return@withLock state.currentLimit
            }

            val orgConfig: OrgRateLimitConfigEntity? = orgRateLimitConfigRepository.findById(org)
            val defaultQps = (orgConfig?.initialQps ?: 10_000)

            val newState = RateState(currentLimit = defaultQps)
            rateLimits[org] = newState
            return@withLock defaultQps
        }
    }

    suspend fun reportSuccess(
        org: String,
        n: Int = defaultN,
        m: Int = defaultM,
        latencyMs: Long = 0,
        thresholdMs: Long = defaultThresholdMs,
    ): Int {
        val mutex = mutexMap.computeIfAbsent(org) { Mutex() }
        return mutex.withLock {
            val state = rateLimits.computeIfAbsent(org) {
                val orgConfig: OrgRateLimitConfigEntity? = runBlocking { orgRateLimitConfigRepository.findById(org) }
                val defaultQps = (orgConfig?.initialQps ?: 10_000)
                RateState(currentLimit = defaultQps)
            }

            if (latencyMs > thresholdMs) {
                return@withLock reportLatencyBasedFailure(state, org)
            }

            if (state.isInSlowStart) {
                state.currentLimit = min(maxLimit, state.currentLimit * n + m)
                if (state.currentLimit >= state.slowStartBaseLine * 2) {
                    state.isInSlowStart = false
                }
            } else {
                state.currentLimit = min(maxLimit, state.currentLimit + m)
            }

            state.failureCount = 0
            state.currentLimit
        }
    }

    suspend fun reportFailure(
        org: String,
        latencyMs: Long = defaultThresholdMs,
        thresholdMs: Long = defaultThresholdMs,
        nThreshold: Int = defaultNThreshold,
        md: Double = defaultMd,
        n: Int = defaultN,
        m: Int = defaultM,
    ): Int {
        val mutex = mutexMap.computeIfAbsent(org) { Mutex() }
        return mutex.withLock {
            val state = rateLimits.computeIfAbsent(org) {
                val orgConfig: OrgRateLimitConfigEntity? = runBlocking { orgRateLimitConfigRepository.findById(org) }
                val defaultQps = (orgConfig?.initialQps ?: 10_000)
                RateState(currentLimit = defaultQps)
            }

            reportLatencyBasedFailure(state, org, md)
        }
    }

    private fun reportLatencyBasedFailure(
        state: RateState,
        org: String,
        md: Double = defaultMd
    ): Int {
        val now = System.currentTimeMillis()
        
        if (now - state.lastFailureTime > 60_000) {
            state.failureCount = 0
        }
        
        state.failureCount++
        state.lastFailureTime = now

        if (state.failureCount >= defaultNThreshold) {
            val beforeLimit = state.currentLimit
            state.currentLimit = max(minLimit, (state.currentLimit * md).toInt())
            
            state.isInSlowStart = true
            state.slowStartBaseLine = state.currentLimit
            state.failureCount = 0
        }

        return state.currentLimit
    }

    fun getRateState(org: String): RateState? = rateLimits[org]

    fun resetState(org: String) {
        rateLimits.remove(org)
        mutexMap.remove(org)
    }
}
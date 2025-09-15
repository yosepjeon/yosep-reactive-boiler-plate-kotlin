package com.yosep.server.common.component.ratelimit.local

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Service

@Service
@ConditionalOnProperty(
    prefix = "ratelimit.local", 
    name = ["enabled"], 
    havingValue = "true", 
    matchIfMissing = false
)
class LocalRateLimitService(
    private val localSlidingWindowRateLimiter: LocalSlidingWindowRateLimiter,
    private val localAimdRateCoordinator: LocalAimdRateCoordinator,
    private val nodeCountCalculator: NodeCountBasedLimitCalculator,
    private val syncScheduler: LocalRateLimitSyncScheduler
) {
    private val logger = LoggerFactory.getLogger(LocalRateLimitService::class.java)

    @Value("\${ratelimit.local.window-ms:1000}")
    private val defaultWindowMs: Int = 1000

    @Value("\${ratelimit.local.latency-threshold-ms:500}")
    private val defaultLatencyThresholdMs: Long = 500

    suspend fun tryAcquire(orgId: String, requestKey: String? = null): AcquisitionResult {
        try {
            val currentLimit = localAimdRateCoordinator.getCurrentLimit(orgId)
            val perNodeLimit = nodeCountCalculator.calculatePerNodeLimit(currentLimit)
            
            val key = buildRateLimitKey(orgId, requestKey)
            val acquired = localSlidingWindowRateLimiter.tryAcquire(key, perNodeLimit, defaultWindowMs)
            
            return AcquisitionResult(
                acquired = acquired,
                currentLimit = currentLimit,
                perNodeLimit = perNodeLimit,
                remainingCount = if (acquired) perNodeLimit - localSlidingWindowRateLimiter.getCurrentCount(key).toInt() else 0
            )
        } catch (e: Exception) {
            logger.warn("Rate limiting failed for org: $orgId", e)
            return AcquisitionResult(acquired = true, currentLimit = Int.MAX_VALUE, perNodeLimit = Int.MAX_VALUE)
        }
    }

    suspend fun reportSuccess(
        orgId: String,
        latencyMs: Long = 0,
        requestKey: String? = null
    ): RateAdjustmentResult {
        try {
            val thresholdMs = defaultLatencyThresholdMs
            val newLimit = localAimdRateCoordinator.reportSuccess(
                org = orgId,
                latencyMs = latencyMs,
                thresholdMs = thresholdMs
            )
            
            val perNodeLimit = nodeCountCalculator.calculatePerNodeLimit(newLimit)
            
            return RateAdjustmentResult(
                success = true,
                newLimit = newLimit,
                perNodeLimit = perNodeLimit,
                adjustmentReason = if (latencyMs > thresholdMs) "latency_failure" else "success"
            )
        } catch (e: Exception) {
            logger.warn("Failed to report success for org: $orgId", e)
            return RateAdjustmentResult(success = false, newLimit = -1, perNodeLimit = -1)
        }
    }

    suspend fun reportFailure(
        orgId: String,
        latencyMs: Long = defaultLatencyThresholdMs,
        requestKey: String? = null
    ): RateAdjustmentResult {
        try {
            val newLimit = localAimdRateCoordinator.reportFailure(
                org = orgId,
                latencyMs = latencyMs,
                thresholdMs = defaultLatencyThresholdMs
            )
            
            val perNodeLimit = nodeCountCalculator.calculatePerNodeLimit(newLimit)
            
            return RateAdjustmentResult(
                success = true,
                newLimit = newLimit,
                perNodeLimit = perNodeLimit,
                adjustmentReason = "failure"
            )
        } catch (e: Exception) {
            logger.warn("Failed to report failure for org: $orgId", e)
            return RateAdjustmentResult(success = false, newLimit = -1, perNodeLimit = -1)
        }
    }

    suspend fun getCurrentLimit(orgId: String): CurrentLimitResult {
        try {
            val currentLimit = localAimdRateCoordinator.getCurrentLimit(orgId)
            val perNodeLimit = nodeCountCalculator.calculatePerNodeLimit(currentLimit)
            val nodeCount = nodeCountCalculator.getNodeCount()
            
            return CurrentLimitResult(
                success = true,
                currentLimit = currentLimit,
                perNodeLimit = perNodeLimit,
                nodeCount = nodeCount
            )
        } catch (e: Exception) {
            logger.warn("Failed to get current limit for org: $orgId", e)
            return CurrentLimitResult(success = false, currentLimit = -1, perNodeLimit = -1, nodeCount = -1)
        }
    }

    fun getCurrentCount(orgId: String, requestKey: String? = null): Long {
        return try {
            val key = buildRateLimitKey(orgId, requestKey)
            localSlidingWindowRateLimiter.getCurrentCount(key)
        } catch (e: Exception) {
            logger.warn("Failed to get current count for org: $orgId", e)
            0L
        }
    }

    fun resetRateLimit(orgId: String, requestKey: String? = null) {
        try {
            val key = buildRateLimitKey(orgId, requestKey)
            localSlidingWindowRateLimiter.reset(key)
            localAimdRateCoordinator.resetState(orgId)
        } catch (e: Exception) {
            logger.warn("Failed to reset rate limit for org: $orgId", e)
        }
    }

    fun forceSync() {
        syncScheduler.forceSyncNow()
    }

    fun getSyncStatus(): Map<String, Any> {
        return syncScheduler.getSyncStatus()
    }

    fun getServiceStatus(): Map<String, Any> {
        return mapOf(
            "localRateLimitEnabled" to true,
            "nodeCount" to nodeCountCalculator.getNodeCount(),
            "syncStatus" to getSyncStatus(),
            "configuration" to mapOf(
                "windowMs" to defaultWindowMs,
                "latencyThresholdMs" to defaultLatencyThresholdMs
            )
        )
    }

    private fun buildRateLimitKey(orgId: String, requestKey: String?): String {
        return if (requestKey.isNullOrBlank()) {
            "local:rate:$orgId"
        } else {
            "local:rate:$orgId:$requestKey"
        }
    }

    data class AcquisitionResult(
        val acquired: Boolean,
        val currentLimit: Int,
        val perNodeLimit: Int,
        val remainingCount: Int = 0
    )

    data class RateAdjustmentResult(
        val success: Boolean,
        val newLimit: Int,
        val perNodeLimit: Int,
        val adjustmentReason: String = ""
    )

    data class CurrentLimitResult(
        val success: Boolean,
        val currentLimit: Int,
        val perNodeLimit: Int,
        val nodeCount: Int
    )
}
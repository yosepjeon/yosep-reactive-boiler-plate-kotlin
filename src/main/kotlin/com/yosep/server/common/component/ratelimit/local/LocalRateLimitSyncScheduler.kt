package com.yosep.server.common.component.ratelimit.local

import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

@Component
class LocalRateLimitSyncScheduler(
    private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
    private val nodeCountCalculator: NodeCountBasedLimitCalculator,
    private val localAimdRateCoordinator: LocalAimdRateCoordinator
) {
    private val logger = LoggerFactory.getLogger(LocalRateLimitSyncScheduler::class.java)

    @Value("\${ratelimit.local.sync-interval-ms:1000}")
    private val syncIntervalMs: Long = 1000

    @Value("\${ratelimit.local.enable-sync:true}")
    private val enableSync: Boolean = true

    private val isRunning = AtomicBoolean(false)
    private var syncJob: Job? = null
    private val lastSyncedLimits = ConcurrentHashMap<String, Int>()
    private val syncInProgress = AtomicBoolean(false)

    @EventListener(ApplicationReadyEvent::class)
    fun startSyncScheduler() {
        if (!enableSync) {
            logger.info("Rate limit DB sync is disabled")
            return
        }

        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting local rate limit sync scheduler with interval: ${syncIntervalMs}ms")
            
            syncJob = CoroutineScope(Dispatchers.IO + SupervisorJob()).launch {
                startPeriodicSync()
            }
        }
    }

    private suspend fun startPeriodicSync() {
        try {
            flow {
                while (isRunning.get()) {
                    emit(Unit)
                    delay(syncIntervalMs)
                }
            }.collect {
                if (syncInProgress.compareAndSet(false, true)) {
                    try {
                        syncRateLimitsFromDB()
                    } catch (e: Exception) {
                        logger.warn("Rate limit sync failed", e)
                    } finally {
                        syncInProgress.set(false)
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Rate limit sync scheduler failed", e)
        }
    }

    private suspend fun syncRateLimitsFromDB() {
        try {
            val configs = orgRateLimitConfigRepository.findAll().toList()
            val currentNodeCount = nodeCountCalculator.getNodeCount()
            
            var syncedCount = 0
            
            configs.forEach { config ->
                val orgId = config.id ?: return@forEach
                val totalLimit = config.maxQps
                val perNodeLimit = nodeCountCalculator.calculatePerNodeLimit(totalLimit)
                
                val lastLimit = lastSyncedLimits[orgId]
                if (lastLimit != perNodeLimit) {
                    runBlocking { updateLocalRateLimit(orgId, perNodeLimit) }
                    lastSyncedLimits[orgId] = perNodeLimit
                    syncedCount++
                }
            }
            
            if (syncedCount > 0) {
                logger.debug("Synced {} rate limits from DB (nodes: {})", syncedCount, currentNodeCount)
            }
        } catch (e: Exception) {
            logger.warn("Failed to sync rate limits from DB", e)
        }
    }

    private suspend fun updateLocalRateLimit(orgId: String, newLimit: Int) {
        val currentState = localAimdRateCoordinator.getRateState(orgId)
        
        if (currentState != null && !isLatencyBasedAdjustmentActive(currentState)) {
            currentState.currentLimit = newLimit
            logger.debug("Updated rate limit for org {} to {} (from DB sync)", orgId, newLimit)
        } else if (currentState == null) {
            localAimdRateCoordinator.getCurrentLimit(orgId)
            val newState = localAimdRateCoordinator.getRateState(orgId)
            newState?.currentLimit = newLimit
            logger.debug("Initialized rate limit for org {} to {} (from DB sync)", orgId, newLimit)
        }
    }

    private fun isLatencyBasedAdjustmentActive(state: LocalAimdRateCoordinator.RateState): Boolean {
        val now = System.currentTimeMillis()
        return (now - state.lastFailureTime < 10000) || state.isInSlowStart
    }

    fun stopSyncScheduler() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping local rate limit sync scheduler")
            syncJob?.cancel()
            syncJob = null
        }
    }

    fun forceSyncNow() {
        if (!enableSync) return
        
        CoroutineScope(Dispatchers.IO).launch {
            if (syncInProgress.compareAndSet(false, true)) {
                try {
                    logger.info("Force syncing rate limits from DB")
                    nodeCountCalculator.forceRefreshNodeCount()
                    syncRateLimitsFromDB()
                    logger.info("Force sync completed")
                } finally {
                    syncInProgress.set(false)
                }
            } else {
                logger.info("Sync already in progress, skipping force sync")
            }
        }
    }

    fun getSyncStatus(): Map<String, Any> {
        return mapOf(
            "isRunning" to isRunning.get(),
            "syncInProgress" to syncInProgress.get(),
            "enableSync" to enableSync,
            "syncIntervalMs" to syncIntervalMs,
            "lastSyncedLimits" to lastSyncedLimits.toMap()
        )
    }
}
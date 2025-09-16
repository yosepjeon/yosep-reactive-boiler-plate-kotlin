package com.yosep.server.common.component.waitqueue

import com.yosep.server.common.component.circuitbreaker.ReactiveRedisCircuitBreakerEventCoordinator
import com.yosep.server.common.component.ratelimit.ReactiveSlidingWindowRateLimiter
import com.yosep.server.common.service.circuitbreaker.ReactiveCircuitBreakerService
import com.yosep.server.common.service.waitqueue.QueueMetrics
import com.yosep.server.common.service.waitqueue.WaitQueueService
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max
import kotlin.math.min

@Component
class WaitQueueCoordinator(
    private val waitQueueService: WaitQueueService,
    private val rateLimiter: ReactiveSlidingWindowRateLimiter,
    private val circuitBreakerService: ReactiveCircuitBreakerService,
    private val circuitBreakerCoordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val properties: WaitQueueProperties
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val isRunning = AtomicBoolean(false)
    private val currentBatchSize = AtomicInteger(properties.initialBatchSize)
    private val backpressureLevel = AtomicInteger(0) // 0: Normal, 1: Medium, 2: High
    private var coordinatorJob: Job? = null

    companion object {
        const val MIN_BATCH_SIZE = 1
        const val MAX_BATCH_SIZE = 1000
        const val BACKPRESSURE_HIGH = 2
        const val BACKPRESSURE_MEDIUM = 1
        const val BACKPRESSURE_NORMAL = 0
    }

    @EventListener(ApplicationReadyEvent::class)
    fun startCoordinator() {
        if (!properties.enabled) {
            logger.info("[WaitQueue] Coordinator is disabled")
            return
        }

        if (isRunning.compareAndSet(false, true)) {
            coordinatorJob = CoroutineScope(Dispatchers.Default + SupervisorJob()).launch {
                logger.info("[WaitQueue] Starting coordinator with interval ${properties.transitionIntervalMs}ms")
                coordinatorLoop()
            }
        }
    }

    private suspend fun coordinatorLoop() {
        while (isRunning.get()) {
            try {
                // 1. 현재 시스템 상태 평가
                val systemHealth = evaluateSystemHealth()

                // 2. 배압 레벨 계산
                val newBackpressureLevel = calculateBackpressureLevel(systemHealth)
                backpressureLevel.set(newBackpressureLevel)

                // 3. 배치 크기 동적 조정
                val adjustedBatchSize = adjustBatchSize(systemHealth, newBackpressureLevel)
                currentBatchSize.set(adjustedBatchSize)

                // 4. 대기열 전환 수행
                if (adjustedBatchSize > 0) {
                    val transitioned = waitQueueService.transitionToEnterQueue(adjustedBatchSize)
                    if (transitioned.isNotEmpty()) {
                        logger.debug("[WaitQueue] Transitioned ${transitioned.size} users to enter queue")

                        // 메트릭 업데이트
                        updateTransitionMetrics(transitioned.size, systemHealth)
                    }
                }

                // 5. 입장열 정리 (오래된 사용자 제거)
                if (System.currentTimeMillis() % 60000 < properties.transitionIntervalMs) {
                    val cleaned = waitQueueService.cleanupEnterQueue(
                        Duration.ofSeconds(properties.enterQueueTtlSeconds)
                    )
                    if (cleaned > 0) {
                        logger.debug("[WaitQueue] Cleaned up $cleaned expired users from enter queue")
                    }
                }

                delay(properties.transitionIntervalMs)
            } catch (e: Exception) {
                logger.error("[WaitQueue] Error in coordinator loop", e)
                delay(properties.transitionIntervalMs * 2) // Back off on error
            }
        }
    }

    /**
     * 시스템 건강도 평가
     */
    private suspend fun evaluateSystemHealth(): SystemHealth {
        val orgCode = "default" // TODO: Make this configurable per organization

        // 1. Rate Limiter 상태 확인
        val rateLimitAvailable = try {
            rateLimiter.tryAcquireSuspend(orgCode, 1, 1000) // Test with 1 permit, 1 second window
        } catch (e: Exception) {
            logger.warn("[WaitQueue] Failed to check rate limiter", e)
            false
        }

        // 2. Circuit Breaker 상태 확인
        val circuitBreakerStates = mutableMapOf<String, String>()
        properties.monitoredCircuitBreakers.forEach { breakerName ->
            try {
                // Circuit Breaker 직접 가져와서 상태 확인
                val cb = circuitBreakerService.getCircuitBreaker(breakerName)
                val state = cb.state.name
                circuitBreakerStates[breakerName] = state
            } catch (e: Exception) {
                logger.warn("[WaitQueue] Failed to get circuit breaker state for $breakerName", e)
                circuitBreakerStates[breakerName] = "ERROR"
            }
        }

        // 3. 현재 대기열 메트릭
        val queueMetrics = try {
            waitQueueService.getQueueMetrics()
        } catch (e: Exception) {
            logger.warn("[WaitQueue] Failed to get queue metrics", e)
            QueueMetrics(0, 0, 0, 0, 0, 0, 0, 60.0)
        }

        // 4. CPU/Memory 사용률 (간단한 예시)
        val runtime = Runtime.getRuntime()
        val memoryUsage = (runtime.totalMemory() - runtime.freeMemory()).toDouble() / runtime.maxMemory()
        val cpuUsage = getProcessCpuLoad()

        return SystemHealth(
            rateLimitAvailable = rateLimitAvailable,
            circuitBreakerStates = circuitBreakerStates,
            queueMetrics = queueMetrics,
            memoryUsage = memoryUsage,
            cpuUsage = cpuUsage,
            timestamp = System.currentTimeMillis()
        )
    }

    /**
     * 배압 레벨 계산
     */
    private fun calculateBackpressureLevel(health: SystemHealth): Int {
        var score = 0

        // Rate Limiter 체크
        if (!health.rateLimitAvailable) {
            score += 3
        }

        // Circuit Breaker 체크
        val openCircuits = health.circuitBreakerStates.values.count { it == "OPEN" }
        val halfOpenCircuits = health.circuitBreakerStates.values.count { it == "HALF_OPEN" }
        score += openCircuits * 2 + halfOpenCircuits

        // Queue 메트릭 체크
        val queueRatio = health.queueMetrics.enterQueueSize.toDouble() /
                        max(properties.maxEnterQueueSize, 1)
        if (queueRatio > 0.9) score += 2
        else if (queueRatio > 0.7) score += 1

        // 시스템 리소스 체크
        if (health.memoryUsage > 0.9 || health.cpuUsage > 0.9) score += 2
        else if (health.memoryUsage > 0.7 || health.cpuUsage > 0.7) score += 1

        return when {
            score >= 5 -> BACKPRESSURE_HIGH
            score >= 2 -> BACKPRESSURE_MEDIUM
            else -> BACKPRESSURE_NORMAL
        }
    }

    /**
     * 배치 크기 동적 조정
     */
    private fun adjustBatchSize(health: SystemHealth, backpressureLevel: Int): Int {
        val currentSize = currentBatchSize.get()

        // 기본 조정 로직
        val targetSize = when (backpressureLevel) {
            BACKPRESSURE_HIGH -> {
                // 높은 배압: 배치 크기를 급격히 감소
                max(MIN_BATCH_SIZE, (currentSize * 0.3).toInt())
            }
            BACKPRESSURE_MEDIUM -> {
                // 중간 배압: 배치 크기를 점진적으로 감소
                max(MIN_BATCH_SIZE, (currentSize * 0.7).toInt())
            }
            else -> {
                // 정상: 배치 크기를 점진적으로 증가
                if (health.rateLimitAvailable && health.queueMetrics.waitQueueSize > 0) {
                    min(MAX_BATCH_SIZE, (currentSize * 1.2).toInt())
                } else {
                    currentSize
                }
            }
        }

        // Circuit Breaker 상태에 따른 추가 조정
        val hasOpenCircuit = health.circuitBreakerStates.values.any { it == "OPEN" }
        if (hasOpenCircuit) {
            return 0 // Circuit이 OPEN이면 전환 중지
        }

        // Rate Limit 가용성에 따른 조정
        if (!health.rateLimitAvailable) {
            return max(MIN_BATCH_SIZE, targetSize / 2)
        }

        // 대기열 크기에 따른 조정
        val waitingUsers = health.queueMetrics.waitQueueSize
        val maxTransition = min(waitingUsers, targetSize)

        return max(MIN_BATCH_SIZE, min(maxTransition, properties.maxBatchSize))
    }

    /**
     * 전환 메트릭 업데이트
     */
    private fun updateTransitionMetrics(transitionCount: Int, health: SystemHealth) {
        logger.info(
            "[WaitQueue] Transition metrics - " +
            "Count: $transitionCount, " +
            "Backpressure: ${backpressureLevel.get()}, " +
            "BatchSize: ${currentBatchSize.get()}, " +
            "WaitQueue: ${health.queueMetrics.waitQueueSize}, " +
            "EnterQueue: ${health.queueMetrics.enterQueueSize}"
        )
    }

    /**
     * CPU 사용률 조회 (간단한 구현)
     */
    private fun getProcessCpuLoad(): Double {
        return try {
            val mxBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean()
            if (mxBean is com.sun.management.OperatingSystemMXBean) {
                mxBean.processCpuLoad
            } else 0.0
        } catch (e: Exception) {
            0.0
        }
    }

    /**
     * 코디네이터 중지
     */
    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            coordinatorJob?.cancel()
            logger.info("[WaitQueue] Coordinator stopped")
        }
    }

    /**
     * 현재 배압 레벨 조회
     */
    fun getBackpressureLevel(): Int = backpressureLevel.get()

    /**
     * 현재 배치 크기 조회
     */
    fun getCurrentBatchSize(): Int = currentBatchSize.get()

    /**
     * 배치 크기 수동 조정
     */
    fun setBatchSize(size: Int) {
        val adjusted = max(MIN_BATCH_SIZE, min(size, MAX_BATCH_SIZE))
        currentBatchSize.set(adjusted)
        logger.info("[WaitQueue] Batch size manually adjusted to $adjusted")
    }
}

data class SystemHealth(
    val rateLimitAvailable: Boolean,
    val circuitBreakerStates: Map<String, String>,
    val queueMetrics: QueueMetrics,
    val memoryUsage: Double,
    val cpuUsage: Double,
    val timestamp: Long
)

@ConfigurationProperties(prefix = "waitqueue")
class WaitQueueProperties {
    var enabled: Boolean = true
    var transitionIntervalMs: Long = 1000
    var initialBatchSize: Int = 100
    var maxBatchSize: Int = 500
    var maxEnterQueueSize: Int = 10000
    var enterQueueTtlSeconds: Long = 3600
    var monitoredCircuitBreakers: List<String> = listOf("default-mydata")
    var backpressureThreshold: Double = 0.8
}
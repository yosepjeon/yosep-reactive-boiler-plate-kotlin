package com.yosep.server.waitqueue.controller

import com.yosep.server.common.service.waitqueue.QueueStatus
import com.yosep.server.common.component.waitqueue.WaitQueueCoordinator
import com.yosep.server.common.service.waitqueue.WaitQueueService
import kotlinx.coroutines.reactor.mono
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/queue")
class WaitQueueController(
    private val waitQueueService: WaitQueueService,
    private val waitQueueCoordinator: WaitQueueCoordinator
) {

    /**
     * 대기열 상태 조회
     */
    @GetMapping("/status")
    fun getQueueStatus(
        @RequestHeader("X-User-Id") userId: String
    ): Mono<ResponseEntity<QueueStatusResponse>> = mono {
        val status = waitQueueService.getQueueStatus(userId)
        ResponseEntity.ok(
            QueueStatusResponse(
                status = status.status,
                position = status.position,
                estimatedWaitTimeSeconds = status.estimatedWaitTime,
                queueSize = status.queueSize,
                message = getStatusMessage(status)
            )
        )
    }

    /**
     * 대기열 참가
     */
    @PostMapping("/join")
    fun joinQueue(
        @RequestHeader("X-User-Id") userId: String,
        @RequestParam(required = false) priority: Double?
    ): Mono<ResponseEntity<JoinQueueResponse>> = mono {
        val position = waitQueueService.joinWaitQueue(
            userId,
            priority ?: System.currentTimeMillis().toDouble()
        )

        when (position) {
            -1L -> {
                ResponseEntity.ok(
                    JoinQueueResponse(
                        success = true,
                        position = 0,
                        message = "You are already in the enter queue",
                        status = "ENTERED"
                    )
                )
            }
            else -> {
                ResponseEntity.ok(
                    JoinQueueResponse(
                        success = true,
                        position = position,
                        message = "Successfully joined the queue at position $position",
                        status = "WAITING",
                        estimatedWaitTimeSeconds = position * 60
                    )
                )
            }
        }
    }

    /**
     * 대기열 이탈
     */
    @DeleteMapping("/leave")
    fun leaveQueue(
        @RequestHeader("X-User-Id") userId: String
    ): Mono<ResponseEntity<LeaveQueueResponse>> = mono {
        waitQueueService.handleUserExit(userId)
        ResponseEntity.ok(
            LeaveQueueResponse(
                success = true,
                message = "Successfully left the queue"
            )
        )
    }

    /**
     * 대기열 메트릭 조회 (관리자용)
     */
    @GetMapping("/metrics")
    fun getQueueMetrics(): Mono<ResponseEntity<QueueMetricsResponse>> = mono {
        val metrics = waitQueueService.getQueueMetrics()
        val backpressureLevel = waitQueueCoordinator.getBackpressureLevel()
        val batchSize = waitQueueCoordinator.getCurrentBatchSize()

        ResponseEntity.ok(
            QueueMetricsResponse(
                waitQueueSize = metrics.waitQueueSize,
                enterQueueSize = metrics.enterQueueSize,
                totalJoined = metrics.totalJoined,
                totalEntered = metrics.totalEntered,
                totalExited = metrics.totalExited,
                lastTransitionTime = metrics.lastTransitionTime,
                lastTransitionCount = metrics.lastTransitionCount,
                avgProcessingTime = metrics.avgProcessingTime,
                currentBackpressureLevel = getBackpressureLevelName(backpressureLevel),
                currentBatchSize = batchSize
            )
        )
    }

    /**
     * 배치 크기 수동 조정 (관리자용)
     */
    @PutMapping("/admin/batch-size")
    fun updateBatchSize(
        @RequestParam size: Int,
        @RequestHeader("X-Admin-Token") adminToken: String
    ): Mono<ResponseEntity<UpdateBatchSizeResponse>> = mono {
        // TODO: Admin token validation
        if (adminToken != "admin-secret") {
            return@mono ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(
                    UpdateBatchSizeResponse(
                        success = false,
                        message = "Invalid admin token"
                    )
                )
        }

        waitQueueCoordinator.setBatchSize(size)
        ResponseEntity.ok(
            UpdateBatchSizeResponse(
                success = true,
                newBatchSize = waitQueueCoordinator.getCurrentBatchSize(),
                message = "Batch size updated successfully"
            )
        )
    }

    private fun getStatusMessage(status: QueueStatus): String {
        return when (status.status) {
            "ENTERED" -> "You have access to the system"
            "WAITING" -> "You are in position ${status.position}. Estimated wait time: ${status.estimatedWaitTime} seconds"
            "NOT_IN_QUEUE" -> "You are not in the queue"
            else -> "Unknown status"
        }
    }

    private fun getBackpressureLevelName(level: Int): String {
        return when (level) {
            WaitQueueCoordinator.Companion.BACKPRESSURE_HIGH -> "HIGH"
            WaitQueueCoordinator.Companion.BACKPRESSURE_MEDIUM -> "MEDIUM"
            WaitQueueCoordinator.Companion.BACKPRESSURE_NORMAL -> "NORMAL"
            else -> "UNKNOWN"
        }
    }
}

// Response DTOs
data class QueueStatusResponse(
    val status: String,
    val position: Long,
    val estimatedWaitTimeSeconds: Long,
    val queueSize: Int,
    val message: String
)

data class JoinQueueResponse(
    val success: Boolean,
    val position: Long,
    val message: String,
    val status: String,
    val estimatedWaitTimeSeconds: Long? = null
)

data class LeaveQueueResponse(
    val success: Boolean,
    val message: String
)

data class QueueMetricsResponse(
    val waitQueueSize: Int,
    val enterQueueSize: Int,
    val totalJoined: Long,
    val totalEntered: Long,
    val totalExited: Long,
    val lastTransitionTime: Long,
    val lastTransitionCount: Int,
    val avgProcessingTime: Double,
    val currentBackpressureLevel: String,
    val currentBatchSize: Int
)

data class UpdateBatchSizeResponse(
    val success: Boolean,
    val newBatchSize: Int? = null,
    val message: String
)
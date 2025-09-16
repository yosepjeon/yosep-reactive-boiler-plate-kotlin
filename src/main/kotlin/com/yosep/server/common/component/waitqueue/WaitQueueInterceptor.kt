package com.yosep.server.common.component.waitqueue

import com.yosep.server.common.service.waitqueue.QueueStatus
import com.yosep.server.common.service.waitqueue.WaitQueueService
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.core.Ordered
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class WaitQueueInterceptor(
    private val waitQueueService: WaitQueueService,
    private val properties: WaitQueueProperties
) : WebFilter, Ordered {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val USER_ID_HEADER = "X-User-Id"
        const val QUEUE_TOKEN_HEADER = "X-Queue-Token"
        const val QUEUE_BYPASS_HEADER = "X-Queue-Bypass"
        val EXCLUDED_PATHS = setOf(
            "/health",
            "/metrics",
            "/actuator",
            "/queue/status",
            "/queue/join",
            "/swagger",
            "/v3/api-docs"
        )
    }

    override fun getOrder(): Int = Ordered.HIGHEST_PRECEDENCE

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        if (!properties.enabled) {
            return chain.filter(exchange)
        }

        val path = exchange.request.path.value()

        // 제외 경로 체크
        if (EXCLUDED_PATHS.any { path.startsWith(it) }) {
            return chain.filter(exchange)
        }

        // 바이패스 헤더 체크 (관리자용)
        if (exchange.request.headers.getFirst(QUEUE_BYPASS_HEADER) == "true") {
            logger.debug("[WaitQueue] Bypassing queue for request: $path")
            return chain.filter(exchange)
        }

        return mono {
            val userId = extractUserId(exchange) ?: run {
                sendErrorResponse(exchange, HttpStatus.UNAUTHORIZED, "User ID is required")
                return@mono null
            }

            // 사용자 대기 상태 확인
            val queueStatus = waitQueueService.getQueueStatus(userId)

            when (queueStatus.status) {
                "ENTERED" -> {
                    // 입장열에 있는 사용자는 통과
                    logger.debug("[WaitQueue] User $userId is in enter queue, allowing request")
                    chain.filter(exchange).awaitSingle()
                }

                "WAITING" -> {
                    // 대기열에 있는 사용자는 대기 상태 응답
                    logger.debug("[WaitQueue] User $userId is waiting at position ${queueStatus.position}")
                    sendWaitingResponse(exchange, queueStatus)
                }

                "NOT_IN_QUEUE" -> {
                    // 대기열에 없는 사용자는 자동으로 대기열에 추가
                    val position = waitQueueService.joinWaitQueue(userId)
                    logger.info("[WaitQueue] User $userId joined queue at position $position")

                    if (position == -1L) {
                        // 이미 입장열에 있는 경우
                        chain.filter(exchange).awaitSingle()
                    } else {
                        // 대기열에 추가된 경우
                        val newStatus = QueueStatus("WAITING", position, position * 60, 0)
                        sendWaitingResponse(exchange, newStatus)
                    }
                }

                else -> {
                    logger.warn("[WaitQueue] Unknown queue status for user $userId: ${queueStatus.status}")
                    sendErrorResponse(exchange, HttpStatus.SERVICE_UNAVAILABLE, "Queue service unavailable")
                }
            }
        }.then()
    }

    private fun extractUserId(exchange: ServerWebExchange): String? {
        // 1. 헤더에서 User ID 추출
        exchange.request.headers.getFirst(USER_ID_HEADER)?.let { return it }

        // 2. JWT 토큰에서 추출 (구현 필요)
        // exchange.request.headers.getFirst("Authorization")?.let {
        //     return extractUserIdFromJwt(it)
        // }

        // 3. 세션에서 추출
        // exchange.session.map { session ->
        //     session.attributes["userId"] as? String
        // }

        // 4. 쿠키에서 추출
        exchange.request.cookies["userId"]?.firstOrNull()?.value?.let { return it }

        return null
    }

    private suspend fun sendWaitingResponse(exchange: ServerWebExchange, status: QueueStatus) {
        val response = exchange.response
        response.statusCode = HttpStatus.SERVICE_UNAVAILABLE
        response.headers.contentType = MediaType.APPLICATION_JSON
        response.headers.add("Retry-After", "5") // 5초 후 재시도

        val responseBody = """
            {
                "status": "waiting",
                "position": ${status.position},
                "estimatedWaitTime": ${status.estimatedWaitTime},
                "queueSize": ${status.queueSize},
                "message": "You are in the waiting queue. Please wait for your turn.",
                "retryAfter": 5
            }
        """.trimIndent()

        val buffer = response.bufferFactory().wrap(responseBody.toByteArray())
        response.writeWith(Mono.just(buffer)).awaitSingle()
    }

    private suspend fun sendErrorResponse(
        exchange: ServerWebExchange,
        status: HttpStatus,
        message: String
    ) {
        val response = exchange.response
        response.statusCode = status
        response.headers.contentType = MediaType.APPLICATION_JSON

        val responseBody = """
            {
                "error": "$message",
                "status": ${status.value()},
                "timestamp": ${System.currentTimeMillis()}
            }
        """.trimIndent()

        val buffer = response.bufferFactory().wrap(responseBody.toByteArray())
        response.writeWith(Mono.just(buffer)).awaitSingle()
    }
}
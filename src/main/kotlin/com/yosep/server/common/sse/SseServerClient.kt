package com.yosep.server.common.sse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import reactor.core.scheduler.Schedulers
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * 서버 간 SSE 통신을 위한 클라이언트
 * 다른 서버의 SSE 스트림을 구독하고 SseEmitter로 릴레이
 */
@Component
class SseServerClient(
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val activeSubscriptions = ConcurrentHashMap<String, Subscription>()

    data class Subscription(
        val id: String,
        val sourceUrl: String,
        val emitter: SseEmitter,
        var isActive: Boolean = true
    )

    /**
     * 원격 SSE 스트림을 구독하고 SseEmitter로 릴레이
     */
    fun relayRemoteStream(
        sourceUrl: String,
        request: ParallelTaskRequest? = null,
        timeout: Long = 60000L
    ): SseEmitter {
        val emitter = SseEmitter(timeout)
        val subscriptionId = UUID.randomUUID().toString()

        val subscription = Subscription(
            id = subscriptionId,
            sourceUrl = sourceUrl,
            emitter = emitter
        )
        activeSubscriptions[subscriptionId] = subscription

        // 에미터 콜백 설정
        setupEmitterCallbacks(emitter, subscriptionId)

        // WebClient 생성
        val webClient = WebClient.builder()
            .baseUrl(extractBaseUrl(sourceUrl))
            .codecs { it.defaultCodecs().maxInMemorySize(10 * 1024 * 1024) }
            .build()

        // SSE 스트림 구독 및 릴레이
        if (request != null) {
            // POST 요청으로 SSE 구독
            subscribeToPostStream(webClient, sourceUrl, request, emitter, subscriptionId)
        } else {
            // GET 요청으로 SSE 구독
            subscribeToGetStream(webClient, sourceUrl, emitter, subscriptionId)
        }

        return emitter
    }

    /**
     * POST 요청으로 SSE 스트림 구독
     */
    private fun subscribeToPostStream(
        webClient: WebClient,
        url: String,
        request: ParallelTaskRequest,
        emitter: SseEmitter,
        subscriptionId: String
    ) {
        webClient.post()
            .uri(extractPath(url))
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToFlux(object : ParameterizedTypeReference<ServerSentEvent<String>>() {})
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                { event ->
                    if (activeSubscriptions[subscriptionId]?.isActive == true) {
                        relayEvent(emitter, event)
                    }
                },
                { error ->
                    handleStreamError(emitter, error, subscriptionId)
                },
                {
                    handleStreamComplete(emitter, subscriptionId)
                }
            )
    }

    /**
     * GET 요청으로 SSE 스트림 구독
     */
    private fun subscribeToGetStream(
        webClient: WebClient,
        url: String,
        emitter: SseEmitter,
        subscriptionId: String
    ) {
        webClient.get()
            .uri(extractPath(url))
            .retrieve()
            .bodyToFlux(object : ParameterizedTypeReference<ServerSentEvent<String>>() {})
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                { event ->
                    if (activeSubscriptions[subscriptionId]?.isActive == true) {
                        relayEvent(emitter, event)
                    }
                },
                { error ->
                    handleStreamError(emitter, error, subscriptionId)
                },
                {
                    handleStreamComplete(emitter, subscriptionId)
                }
            )
    }

    /**
     * 병렬로 여러 SSE 스트림을 구독하고 하나의 SseEmitter로 통합
     */
    fun aggregateMultipleStreams(
        sourceUrls: List<String>,
        timeout: Long = 120000L
    ): SseEmitter {
        val emitter = SseEmitter(timeout)
        val aggregationId = UUID.randomUUID().toString()
        val completedStreams = ConcurrentHashMap<String, Boolean>()

        setupEmitterCallbacks(emitter, aggregationId)

        sourceUrls.forEach { url ->
            val streamId = UUID.randomUUID().toString()
            completedStreams[streamId] = false

            val webClient = WebClient.builder()
                .baseUrl(extractBaseUrl(url))
                .build()

            webClient.get()
                .uri(extractPath(url))
                .retrieve()
                .bodyToFlux(object : ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .subscribeOn(Schedulers.parallel())
                .subscribe(
                    { event ->
                        try {
                            // 스트림 ID를 추가하여 이벤트 릴레이
                            val modifiedEvent = SseEmitter.event()
                                .id(event.id() ?: UUID.randomUUID().toString())
                                .name(event.event() ?: "data")
                                .data(mapOf(
                                    "streamId" to streamId,
                                    "sourceUrl" to url,
                                    "data" to event.data()
                                ), MediaType.APPLICATION_JSON)

                            emitter.send(modifiedEvent)
                        } catch (e: Exception) {
                            logger.error("Failed to relay aggregated event", e)
                        }
                    },
                    { error ->
                        logger.error("Stream error from $url", error)
                        completedStreams[streamId] = true
                        checkAllStreamsComplete(completedStreams, emitter)
                    },
                    {
                        logger.info("Stream completed from $url")
                        completedStreams[streamId] = true
                        checkAllStreamsComplete(completedStreams, emitter)
                    }
                )
        }

        return emitter
    }

    /**
     * SSE 이벤트를 SseEmitter로 릴레이
     */
    private fun relayEvent(emitter: SseEmitter, event: ServerSentEvent<String>) {
        try {
            val sseEvent = SseEmitter.event()
                .id(event.id() ?: UUID.randomUUID().toString())
                .name(event.event() ?: "data")

            // 데이터 파싱 및 전송
            event.data()?.let { data ->
                try {
                    // JSON으로 파싱 시도
                    val parsedData = objectMapper.readValue<Map<String, Any>>(data)
                    sseEvent.data(parsedData, MediaType.APPLICATION_JSON)
                } catch (e: Exception) {
                    // 파싱 실패 시 원본 문자열 전송
                    sseEvent.data(data)
                }
            }

            event.comment()?.let { sseEvent.comment(it) }
            event.retry()?.let { sseEvent.reconnectTime(it.toMillis()) }

            emitter.send(sseEvent)
        } catch (e: Exception) {
            logger.error("Failed to relay SSE event", e)
            throw e
        }
    }

    /**
     * 에미터 콜백 설정
     */
    private fun setupEmitterCallbacks(emitter: SseEmitter, subscriptionId: String) {
        emitter.onCompletion {
            logger.info("SSE relay completed: $subscriptionId")
            activeSubscriptions[subscriptionId]?.isActive = false
            activeSubscriptions.remove(subscriptionId)
        }

        emitter.onTimeout {
            logger.warn("SSE relay timeout: $subscriptionId")
            activeSubscriptions[subscriptionId]?.isActive = false
            activeSubscriptions.remove(subscriptionId)
        }

        emitter.onError { error ->
            logger.error("SSE relay error: $subscriptionId", error)
            activeSubscriptions[subscriptionId]?.isActive = false
            activeSubscriptions.remove(subscriptionId)
        }
    }

    /**
     * 스트림 에러 처리
     */
    private fun handleStreamError(emitter: SseEmitter, error: Throwable, subscriptionId: String) {
        logger.error("Remote stream error: $subscriptionId", error)

        try {
            val errorEvent = SseEmitter.event()
                .id(UUID.randomUUID().toString())
                .name("error")
                .data(mapOf(
                    "error" to (error.message ?: "Unknown error"),
                    "timestamp" to System.currentTimeMillis()
                ), MediaType.APPLICATION_JSON)

            emitter.send(errorEvent)
        } catch (e: Exception) {
            logger.error("Failed to send error event", e)
        }

        activeSubscriptions[subscriptionId]?.isActive = false
        emitter.completeWithError(error)
    }

    /**
     * 스트림 완료 처리
     */
    private fun handleStreamComplete(emitter: SseEmitter, subscriptionId: String) {
        logger.info("Remote stream completed: $subscriptionId")

        try {
            val completeEvent = SseEmitter.event()
                .id(UUID.randomUUID().toString())
                .name("complete")
                .data(mapOf(
                    "message" to "Stream completed",
                    "timestamp" to System.currentTimeMillis()
                ), MediaType.APPLICATION_JSON)

            emitter.send(completeEvent)
        } catch (e: Exception) {
            logger.error("Failed to send complete event", e)
        }

        activeSubscriptions[subscriptionId]?.isActive = false
        emitter.complete()
    }

    /**
     * 모든 스트림 완료 확인
     */
    private fun checkAllStreamsComplete(
        completedStreams: ConcurrentHashMap<String, Boolean>,
        emitter: SseEmitter
    ) {
        if (completedStreams.values.all { it }) {
            try {
                val summaryEvent = SseEmitter.event()
                    .id(UUID.randomUUID().toString())
                    .name("aggregate_complete")
                    .data(mapOf(
                        "message" to "All streams completed",
                        "totalStreams" to completedStreams.size,
                        "timestamp" to System.currentTimeMillis()
                    ), MediaType.APPLICATION_JSON)

                emitter.send(summaryEvent)
                emitter.complete()
            } catch (e: Exception) {
                logger.error("Failed to send aggregate complete event", e)
            }
        }
    }

    private fun extractBaseUrl(url: String): String {
        val parts = url.split("/")
        return "${parts[0]}//${parts[2]}"
    }

    private fun extractPath(url: String): String {
        val parts = url.split("/", limit = 4)
        return if (parts.size > 3) "/${parts[3]}" else "/"
    }

    /**
     * 활성 구독 상태 조회
     */
    fun getActiveSubscriptions(): Map<String, Any> {
        return mapOf(
            "activeCount" to activeSubscriptions.size,
            "subscriptions" to activeSubscriptions.values.map { sub ->
                mapOf(
                    "id" to sub.id,
                    "sourceUrl" to sub.sourceUrl,
                    "isActive" to sub.isActive
                )
            }
        )
    }

    /**
     * 특정 구독 취소
     */
    fun cancelSubscription(subscriptionId: String): Boolean {
        val subscription = activeSubscriptions[subscriptionId]
        return if (subscription != null) {
            subscription.isActive = false
            subscription.emitter.complete()
            activeSubscriptions.remove(subscriptionId)
            true
        } else {
            false
        }
    }
}
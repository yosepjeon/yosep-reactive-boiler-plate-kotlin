package com.yosep.server.sse.controller

import com.yosep.server.common.sse.ParallelApiService
import com.yosep.server.common.sse.ParallelTaskRequest
import com.yosep.server.common.sse.SseEvent
import com.yosep.server.common.sse.TaskConfig
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.*

@RestController
@RequestMapping("/api/sse")
class SseParallelController(
    private val parallelApiService: ParallelApiService
) {

    /**
     * SSE로 병렬 API 호출 결과를 스트리밍
     * 각 응답을 개별적으로 받고, 마지막에 전체 결과를 받음
     */
    @PostMapping("/parallel-tasks", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun executeParallelTasks(@RequestBody request: ParallelTaskRequest): Flux<ServerSentEvent<SseEvent>> {
        return parallelApiService.executeParallelTasks(request)
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .build()
            }
    }

    /**
     * 진행 상황을 포함한 병렬 태스크 실행
     */
    @PostMapping("/parallel-tasks-progress", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun executeParallelTasksWithProgress(@RequestBody request: ParallelTaskRequest): Flux<ServerSentEvent<SseEvent>> {
        return parallelApiService.executeParallelTasksWithProgress(request)
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .build()
            }
    }

    /**
     * 테스트용 샘플 병렬 태스크 실행
     */
    @GetMapping("/sample-parallel", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun sampleParallelExecution(): Flux<ServerSentEvent<SseEvent>> {
        // 샘플 태스크 생성
        val sampleRequest = ParallelTaskRequest(
            tasks = listOf(
                TaskConfig(
                    id = "task1",
                    name = "Get User Data",
                    url = "https://jsonplaceholder.typicode.com/users/1",
                    timeoutMs = 5000
                ),
                TaskConfig(
                    id = "task2",
                    name = "Get Posts",
                    url = "https://jsonplaceholder.typicode.com/posts?userId=1",
                    timeoutMs = 5000
                ),
                TaskConfig(
                    id = "task3",
                    name = "Get Comments",
                    url = "https://jsonplaceholder.typicode.com/comments?postId=1",
                    timeoutMs = 5000
                ),
                TaskConfig(
                    id = "task4",
                    name = "Get Albums",
                    url = "https://jsonplaceholder.typicode.com/albums?userId=1",
                    timeoutMs = 5000
                )
            )
        )

//        val sampleRequest = ParallelTaskRequest(
//            tasks = listOf(
//                TaskConfig(
//                    id = "task1",
//                    name = "Get User Data",
//                    url = "http://localhost:20001//api/sse-proxy/test/1",
//                    timeoutMs = 5000
//                ),
//                TaskConfig(
//                    id = "task2",
//                    name = "Get Posts",
//                    url = "http://localhost:20001//api/sse-proxy/test/2",
//                    timeoutMs = 5000
//                ),
//                TaskConfig(
//                    id = "task3",
//                    name = "Get Comments",
//                    url = "http://localhost:20001//api/sse-proxy/test/3",
//                    timeoutMs = 5000
//                ),
//                TaskConfig(
//                    id = "task4",
//                    name = "Get Albums",
//                    url = "http://localhost:20001//api/sse-proxy/test/4",
//                    timeoutMs = 5000
//                )
//            )
//        )

        return parallelApiService.executeParallelTasksWithProgress(sampleRequest)
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .comment("Sample parallel execution")
                    .build()
            }
    }

    /**
     * Coroutine Flow 기반 SSE 예제
     */
    @GetMapping("/flow-example", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    suspend fun flowExample(): Flow<ServerSentEvent<Map<String, Any>>> {
        return Flux.interval(Duration.ofSeconds(1))
            .take(10)
            .map { tick ->
                ServerSentEvent.builder<Map<String, Any>>()
                    .id(UUID.randomUUID().toString())
                    .event("tick")
                    .data(mapOf(
                        "count" to tick,
                        "message" to "Flow tick $tick",
                        "timestamp" to System.currentTimeMillis()
                    ))
                    .build()
            }
            .asFlow()
    }

    /**
     * 커스텀 병렬 실행 with 타임아웃
     */
    @PostMapping("/parallel-with-timeout", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun parallelWithTimeout(
        @RequestBody request: ParallelTaskRequest,
        @RequestParam(defaultValue = "30000") totalTimeoutMs: Long
    ): Flux<ServerSentEvent<SseEvent>> {
        return parallelApiService.executeParallelTasks(request)
            .timeout(Duration.ofMillis(totalTimeoutMs))
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .build()
            }
            .onErrorResume { error ->
                Flux.just(
                    ServerSentEvent.builder<SseEvent>()
                        .id(UUID.randomUUID().toString())
                        .event("ERROR")
                        .data(
                            SseEvent.Error(
                                id = UUID.randomUUID().toString(),
                                taskId = "system",
                                taskName = "Parallel Execution",
                                error = "Timeout exceeded: ${error.message}",
                                retryable = false
                            )
                        )
                        .build()
                )
            }
    }

    private fun getEventName(event: SseEvent): String {
        return when (event) {
            is SseEvent.Progress -> "progress"
            is SseEvent.Success -> "success"
            is SseEvent.Error -> "error"
            is SseEvent.Complete -> "complete"
        }
    }
}
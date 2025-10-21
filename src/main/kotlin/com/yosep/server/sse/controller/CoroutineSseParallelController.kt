package com.yosep.server.sse.controller

import com.yosep.server.common.sse.CoroutineParallelApiService
import com.yosep.server.common.sse.ParallelTaskRequest
import com.yosep.server.common.sse.SseEvent
import com.yosep.server.common.sse.TaskConfig
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/coroutine-sse")
class CoroutineSseParallelController(
    private val coroutineParallelApiService: CoroutineParallelApiService
) {

    /**
     * 코루틴 Flow 기반 병렬 태스크 실행 (SSE)
     */
    @PostMapping("/parallel-tasks", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun executeParallelTasks(@RequestBody request: ParallelTaskRequest): Flow<ServerSentEvent<SseEvent>> {
        return coroutineParallelApiService.executeParallelTasksFlow(request)
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .build()
            }
    }

    /**
     * 샘플 병렬 태스크 실행 (코루틴)
     */
    @GetMapping("/sample-parallel", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun sampleParallelExecution(): Flow<ServerSentEvent<SseEvent>> {
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

        return coroutineParallelApiService.executeParallelTasksFlow(sampleRequest)
            .map { event ->
                ServerSentEvent.builder<SseEvent>()
                    .id(event.id)
                    .event(getEventName(event))
                    .data(event)
                    .comment("Sample parallel execution (coroutine)")
                    .build()
            }
    }

    private fun getEventName(event: SseEvent): String = when (event) {
        is SseEvent.Progress -> "progress"
        is SseEvent.Success -> "success"
        is SseEvent.Error -> "error"
        is SseEvent.Complete -> "complete"
    }
}

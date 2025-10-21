package com.yosep.server.common.sse

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * Kotlin Coroutine 기반의 병렬 API 실행 서비스.
 * 기존 Reactor 기반 ParallelApiService 는 그대로 두고, 코루틴/Flow 기반 대안을 제공합니다.
 */
@Service
class CoroutineParallelApiService {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val webClient: WebClient = WebClient.builder()
        .codecs { configurer ->
            configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)
        }
        .build()

    /**
     * 코루틴과 Flow를 사용하여 병렬 태스크를 실행하고 SSE 이벤트를 흘려보냅니다.
     * - 각 태스크는 개별 코루틴에서 실행됩니다.
     * - STARTED, SUCCESS/ERROR 이벤트를 태스크마다 전송하고, 마지막에 COMPLETE 이벤트를 전송합니다.
     */
    fun executeParallelTasksFlow(request: ParallelTaskRequest): Flow<SseEvent> = channelFlow {
        val startTime = System.currentTimeMillis()
        val results = ConcurrentHashMap<String, Any>()

        // 각 Task 를 별도 코루틴으로 실행
        coroutineScope {
            request.tasks.forEach { task ->
                launch(Dispatchers.IO) {
                    // STARTED 이벤트
                    trySend(
                        SseEvent.Progress(
                            id = UUID.randomUUID().toString(),
                            timestamp = Instant.now(),
                            taskId = task.id,
                            taskName = task.name,
                            status = "STARTED",
                            progressPercent = 0
                        )
                    )

                    // 실행 및 재시도
                    val result = runTaskWithRetry(task)
                    if (result != null) {
                        results[task.id] = result
                        trySend(
                            SseEvent.Success(
                                id = UUID.randomUUID().toString(),
                                timestamp = Instant.now(),
                                taskId = task.id,
                                taskName = task.name,
                                result = result,
                                executionTimeMs = System.currentTimeMillis() - startTime
                            )
                        )
                    } else {
                        val errorMsg = "Task ${task.id} failed after ${task.retryCount} retries"
                        results[task.id] = mapOf("error" to errorMsg)
                        trySend(
                            SseEvent.Error(
                                id = UUID.randomUUID().toString(),
                                timestamp = Instant.now(),
                                taskId = task.id,
                                taskName = task.name,
                                error = errorMsg,
                                retryable = task.retryCount > 0
                            )
                        )
                    }
                }
            }
        }

        // 모든 자식 코루틴 완료 후 COMPLETE 전송
        val successCount = results.values.count { it !is Map<*, *> || !it.containsKey("error") }
        val errorCount = request.tasks.size - successCount
        trySend(
            SseEvent.Complete(
                id = UUID.randomUUID().toString(),
                timestamp = Instant.now(),
                totalTasks = request.tasks.size,
                successCount = successCount,
                errorCount = errorCount,
                aggregatedResults = results.toMap(),
                totalExecutionTimeMs = System.currentTimeMillis() - startTime
            )
        )
        close()
    }

    /**
     * 태스크를 timeout, retry 정책과 함께 실행합니다. 성공 시 결과를, 실패 시 null을 반환합니다.
     */
    private suspend fun runTaskWithRetry(task: TaskConfig): Any? {
        var lastError: Throwable? = null
        val attempts = task.retryCount.coerceAtLeast(0) + 1
        repeat(attempts) { attempt ->
            try {
                return withTimeout(task.timeoutMs) {
                    executeTask(task)
                }
            } catch (e: TimeoutCancellationException) {
                lastError = e
                logger.warn("Task ${task.id} timed out on attempt ${attempt + 1}")
            } catch (e: Throwable) {
                lastError = e
                logger.warn("Task ${task.id} failed on attempt ${attempt + 1}: ${e.message}")
            }
        }
        logger.error("Task ${task.id} failed after $attempts attempts", lastError)
        return null
    }

    /**
     * 실제 HTTP 호출을 수행합니다. suspend 함수로 구현하여 코루틴에서 사용할 수 있습니다.
     */
    private suspend fun executeTask(task: TaskConfig): Any = withContext(Dispatchers.IO) {
        val requestSpec = webClient
            .method(HttpMethod.valueOf(task.method))
            .uri(task.url)
            .headers { headers ->
                task.headers.forEach { (k, v) -> headers.set(k, v) }
            }

        val specWithBody = if (task.body != null) {
            requestSpec.bodyValue(task.body)
        } else requestSpec

        // kotlinx-coroutines-reactor 의 awaitBody 확장을 사용
        specWithBody.retrieve().awaitBody<Any>()
    }
}

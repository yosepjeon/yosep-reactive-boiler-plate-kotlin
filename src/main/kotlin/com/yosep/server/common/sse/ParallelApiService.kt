package com.yosep.server.common.sse

import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Service
class ParallelApiService {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val webClient: WebClient = WebClient.builder()
        .codecs { configurer ->
            configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)
        }
        .build()

    /**
     * 병렬 API 호출을 실행하고 SSE로 진행 상황을 스트리밍
     */
    fun executeParallelTasks(request: ParallelTaskRequest): Flux<SseEvent> {
        val sink = Sinks.many().multicast().onBackpressureBuffer<SseEvent>()
        val results = ConcurrentHashMap<String, Any>()
        val startTime = System.currentTimeMillis()

        // 각 태스크를 병렬로 실행
        val taskFluxes = request.tasks.map { task ->
            executeTask(task)
                .doOnSubscribe {
                    // 태스크 시작 알림
                    sink.tryEmitNext(
                        SseEvent.Progress(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            status = "STARTED",
                            progressPercent = 0
                        )
                    )
                }
                .doOnNext { result ->
                    // 태스크 성공 알림
                    results[task.id] = result
                    sink.tryEmitNext(
                        SseEvent.Success(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            result = result,
                            executionTimeMs = System.currentTimeMillis() - startTime
                        )
                    )
                }
                .doOnError { error ->
                    // 태스크 실패 알림
                    logger.error("Task ${task.id} failed", error)
                    results[task.id] = mapOf("error" to error.message)
                    sink.tryEmitNext(
                        SseEvent.Error(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            error = error.message ?: "Unknown error",
                            retryable = task.retryCount > 0
                        )
                    )
                }
                .onErrorResume { Mono.empty() }
        }

        // 모든 태스크 병렬 실행
        Flux.merge(*taskFluxes.toTypedArray())
            .subscribeOn(Schedulers.parallel())
            .doOnComplete {
                // 모든 태스크 완료 후 최종 결과 전송
                val successCount = results.values.count { it !is Map<*, *> || !it.containsKey("error") }
                val errorCount = request.tasks.size - successCount

                sink.tryEmitNext(
                    SseEvent.Complete(
                        id = UUID.randomUUID().toString(),
                        totalTasks = request.tasks.size,
                        successCount = successCount,
                        errorCount = errorCount,
                        aggregatedResults = results.toMap(),
                        totalExecutionTimeMs = System.currentTimeMillis() - startTime
                    )
                )
                sink.tryEmitComplete()
            }
            .subscribe()

        return sink.asFlux()
    }

    /**
     * 개별 태스크 실행
     */
    private fun executeTask(task: TaskConfig): Mono<Any> {
        val requestSpec = webClient
            .method(HttpMethod.valueOf(task.method))
            .uri(task.url)
            .headers { headers ->
                task.headers.forEach { (key, value) ->
                    headers.set(key, value)
                }
            }

        // body가 있는 경우 추가
        val specWithBody = if (task.body != null) {
            requestSpec.bodyValue(task.body)
        } else {
            requestSpec
        }

        return specWithBody
            .retrieve()
            .bodyToMono(Map::class.java)
            .timeout(Duration.ofMillis(task.timeoutMs))
            .retry(task.retryCount.toLong())
            .map { it as Any }
    }

    /**
     * 진행 상황을 주기적으로 업데이트하는 병렬 태스크 실행
     */
    fun executeParallelTasksWithProgress(request: ParallelTaskRequest): Flux<SseEvent> {
        val sink = Sinks.many().multicast().onBackpressureBuffer<SseEvent>()
        val results = ConcurrentHashMap<String, Any>()
        val taskProgress = ConcurrentHashMap<String, Int>()
        val startTime = System.currentTimeMillis()

        val taskFluxes = request.tasks.map { task ->
            taskProgress[task.id] = 0

            // 진행 상황 시뮬레이션을 위한 Flux
            val progressFlux = Flux.interval(Duration.ofMillis(500))
                .take(10)
                .doOnNext { tick ->
                    val progress = ((tick + 1) * 10).toInt()
                    taskProgress[task.id] = progress

                    sink.tryEmitNext(
                        SseEvent.Progress(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            status = "PROCESSING",
                            progressPercent = progress,
                            data = mapOf("step" to "Processing chunk ${tick + 1}")
                        )
                    )
                }

            // 실제 API 호출과 진행 상황 결합
            Flux.zip(
                progressFlux.then(Mono.empty<Unit>()),
                executeTaskWithRetry(task)
            )
                .map { it.t2 }
                .doOnNext { result ->
                    results[task.id] = result
                    taskProgress[task.id] = 100

                    sink.tryEmitNext(
                        SseEvent.Success(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            result = result,
                            executionTimeMs = System.currentTimeMillis() - startTime
                        )
                    )
                }
                .doOnError { error ->
                    logger.error("Task ${task.id} failed", error)
                    results[task.id] = mapOf("error" to error.message)

                    sink.tryEmitNext(
                        SseEvent.Error(
                            id = UUID.randomUUID().toString(),
                            taskId = task.id,
                            taskName = task.name,
                            error = error.message ?: "Unknown error",
                            retryable = true
                        )
                    )
                }
                .onErrorResume { Mono.empty() }
        }

        // 병렬 실행
        Flux.merge(*taskFluxes.toTypedArray())
            .subscribeOn(Schedulers.parallel())
            .doOnComplete {
                val successCount = results.values.count { it !is Map<*, *> || !it.containsKey("error") }
                val errorCount = request.tasks.size - successCount

                sink.tryEmitNext(
                    SseEvent.Complete(
                        id = UUID.randomUUID().toString(),
                        totalTasks = request.tasks.size,
                        successCount = successCount,
                        errorCount = errorCount,
                        aggregatedResults = results.toMap(),
                        totalExecutionTimeMs = System.currentTimeMillis() - startTime
                    )
                )
                sink.tryEmitComplete()
            }
            .subscribe()

        return sink.asFlux()
    }

    private fun executeTaskWithRetry(task: TaskConfig): Mono<Any> {
        return executeTask(task)
            .retryWhen(
                reactor.util.retry.Retry.backoff(task.retryCount.toLong(), Duration.ofSeconds(1))
                    .maxBackoff(Duration.ofSeconds(10))
            )
    }
}
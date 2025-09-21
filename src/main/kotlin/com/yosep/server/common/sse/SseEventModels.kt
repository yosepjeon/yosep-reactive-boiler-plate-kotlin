package com.yosep.server.common.sse

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Instant

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = SseEvent.Progress::class, name = "PROGRESS"),
    JsonSubTypes.Type(value = SseEvent.Success::class, name = "SUCCESS"),
    JsonSubTypes.Type(value = SseEvent.Error::class, name = "ERROR"),
    JsonSubTypes.Type(value = SseEvent.Complete::class, name = "COMPLETE")
)
sealed class SseEvent {
    abstract val id: String
    abstract val timestamp: Instant

    data class Progress(
        override val id: String,
        override val timestamp: Instant = Instant.now(),
        val taskId: String,
        val taskName: String,
        val status: String,
        val data: Any? = null,
        val progressPercent: Int? = null
    ) : SseEvent()

    data class Success(
        override val id: String,
        override val timestamp: Instant = Instant.now(),
        val taskId: String,
        val taskName: String,
        val result: Any,
        val executionTimeMs: Long
    ) : SseEvent()

    data class Error(
        override val id: String,
        override val timestamp: Instant = Instant.now(),
        val taskId: String,
        val taskName: String,
        val error: String,
        val retryable: Boolean = true
    ) : SseEvent()

    data class Complete(
        override val id: String,
        override val timestamp: Instant = Instant.now(),
        val totalTasks: Int,
        val successCount: Int,
        val errorCount: Int,
        val aggregatedResults: Map<String, Any>,
        val totalExecutionTimeMs: Long
    ) : SseEvent()
}

data class ParallelTaskRequest(
    val tasks: List<TaskConfig>
)

data class TaskConfig(
    val id: String,
    val name: String,
    val url: String,
    val method: String = "GET",
    val headers: Map<String, String> = emptyMap(),
    val body: Any? = null,
    val timeoutMs: Long = 5000,
    val retryCount: Int = 0
)
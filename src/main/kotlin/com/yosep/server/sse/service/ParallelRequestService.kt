package com.yosep.server.sse.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.util.*

@Service
class ParallelRequestService(
    private val webClientBuilder: WebClient.Builder
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Execute parallel requests and emit responses as Flow
     */
    suspend fun executeParallelRequests(
        baseUrl: String,
        endpoints: List<String>
    ): Flow<Response> = flow {
        val webClient = webClientBuilder
            .baseUrl(baseUrl)
            .build()

        val results = mutableListOf<IndividualResponse>()

        // Execute parallel requests
        coroutineScope {
            endpoints.map { endpoint ->
                async {
                    try {
                        val requestId = UUID.randomUUID().toString()
                        logger.info("Starting request $requestId to $endpoint")

                        // Simulate actual API call (replace with real implementation)
                        val response = makeRequest(webClient, endpoint, requestId)

                        // Emit individual response immediately
                        emit(response)

                        response
                    } catch (e: Exception) {
                        logger.error("Error in request to $endpoint", e)
                        val errorResponse = ErrorResponse(
                            id = UUID.randomUUID().toString(),
                            endpoint = endpoint,
                            error = e.message ?: "Unknown error",
                            timestamp = System.currentTimeMillis()
                        )
                        emit(errorResponse)
                        null
                    }
                }
            }.awaitAll().filterNotNull().let {
                results.addAll(it)
            }
        }

        // Emit aggregated response
        val aggregatedResponse = AggregatedResponse(
            totalRequests = endpoints.size,
            successfulRequests = results.size,
            failedRequests = endpoints.size - results.size,
            responses = results,
            processingTimeMs = results.maxOfOrNull { it.responseTimeMs } ?: 0,
            timestamp = System.currentTimeMillis()
        )

        emit(aggregatedResponse)
    }.flowOn(Dispatchers.IO)

    /**
     * Make actual HTTP request
     */
    private suspend fun makeRequest(
        webClient: WebClient,
        endpoint: String,
        requestId: String
    ): IndividualResponse {
        val startTime = System.currentTimeMillis()

        return try {
            // Simulate different response times
            delay((100..1000).random().toLong())

            // For demo purposes, return mock data
            // In real implementation, use: webClient.get().uri(endpoint).awaitBody<Map<String, Any>>()
            val mockData = mapOf(
                "endpoint" to endpoint,
                "data" to "Response from $endpoint",
                "randomValue" to (1..100).random(),
                "timestamp" to System.currentTimeMillis()
            )

            IndividualResponse(
                id = requestId,
                endpoint = endpoint,
                data = mockData,
                responseTimeMs = System.currentTimeMillis() - startTime,
                timestamp = System.currentTimeMillis()
            )
        } catch (e: Exception) {
            throw Exception("Failed to call $endpoint: ${e.message}")
        }
    }

    /**
     * Execute parallel requests with retry logic
     */
    suspend fun executeParallelRequestsWithRetry(
        baseUrl: String,
        endpoints: List<String>,
        maxRetries: Int = 3
    ): Flow<Response> = flow {
        val webClient = webClientBuilder
            .baseUrl(baseUrl)
            .codecs { it.defaultCodecs().maxInMemorySize(1024 * 1024) }
            .build()

        val results = mutableListOf<IndividualResponse>()
        val errors = mutableListOf<ErrorResponse>()

        coroutineScope {
            endpoints.map { endpoint ->
                async {
                    var lastException: Exception? = null
                    repeat(maxRetries) { attempt ->
                        try {
                            val requestId = "${UUID.randomUUID()}-attempt-${attempt + 1}"
                            val response = makeRequest(webClient, endpoint, requestId)
                            emit(response)
                            return@async response
                        } catch (e: Exception) {
                            lastException = e
                            if (attempt < maxRetries - 1) {
                                delay(1000L * (attempt + 1)) // Exponential backoff
                            }
                        }
                    }

                    // All retries failed
                    val errorResponse = ErrorResponse(
                        id = UUID.randomUUID().toString(),
                        endpoint = endpoint,
                        error = lastException?.message ?: "Max retries exceeded",
                        timestamp = System.currentTimeMillis()
                    )
                    emit(errorResponse)
                    errors.add(errorResponse)
                    null
                }
            }.awaitAll().filterNotNull().let {
                results.addAll(it)
            }
        }

        // Emit final aggregated response
        emit(
            AggregatedResponse(
                totalRequests = endpoints.size,
                successfulRequests = results.size,
                failedRequests = errors.size,
                responses = results,
                errors = errors,
                processingTimeMs = results.maxOfOrNull { it.responseTimeMs } ?: 0,
                timestamp = System.currentTimeMillis()
            )
        )
    }

    // Response types
    sealed interface Response

    data class IndividualResponse(
        val id: String,
        val endpoint: String,
        val data: Map<String, Any>,
        val responseTimeMs: Long,
        val timestamp: Long
    ) : Response

    data class ErrorResponse(
        val id: String,
        val endpoint: String,
        val error: String,
        val timestamp: Long
    ) : Response

    data class AggregatedResponse(
        val totalRequests: Int,
        val successfulRequests: Int,
        val failedRequests: Int,
        val responses: List<IndividualResponse>,
        val errors: List<ErrorResponse> = emptyList(),
        val processingTimeMs: Long,
        val timestamp: Long
    ) : Response
}
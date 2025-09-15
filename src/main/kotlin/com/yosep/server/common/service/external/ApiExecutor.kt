package com.yosep.server.common.service.external

import com.yosep.server.common.component.external.OrgWebClientManager
import com.yosep.server.common.component.ratelimit.ReactiveAimdSlowStartRateCoordinator
import com.yosep.server.common.component.ratelimit.ReactiveSlidingWindowRateLimiter
import com.yosep.server.common.exception.YosepHttpErrorException
import com.yosep.server.common.service.circuitbreaker.ReactiveCircuitBreakerService
import com.yosep.server.domain.external.ApiRequest
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.boot.configurationprocessor.json.JSONObject
import org.springframework.http.*
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

@Component
class ApiExecutor(
    private val orgWebClientManager: OrgWebClientManager,
    private val aimdCoordinator: ReactiveAimdSlowStartRateCoordinator,
    private val slidingLimiter: ReactiveSlidingWindowRateLimiter,
    private val cbService: ReactiveCircuitBreakerService,
) {
    private val log = LoggerFactory.getLogger(ApiExecutor::class.java)

    private val WEBCLIENT_SUFFIX = "-mydata"

    // Suspend-friendly execute API
    suspend fun <T> execute(apiRequest: ApiRequest<T>): ResponseEntity<JSONObject?> {
        val orgCode = apiRequest.orgCode ?: return ResponseEntity.badRequest().build()

        // 1) Rate limit: current limit + sliding window acquisition 체크
        val maxQps = aimdCoordinator.getCurrentLimit(orgCode)
        println("maxQps for $orgCode: $maxQps")
        val acquired = slidingLimiter.tryAcquireSuspend(orgCode, maxQps, 1000)
        if (!acquired) {
            val body = JSONObject()
            try {
                body.put("error", "Rate limit exceeded")
                body.put("orgCode", orgCode)
            } catch (e: Exception) {
                log.error("Failed to create rate limit response", e)
            }
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(body)
        }

        // 2) CircuitBreaker permission 여부 체크
        val breakerName = "$orgCode$WEBCLIENT_SUFFIX"
        val cb: CircuitBreaker = cbService.getCircuitBreaker(breakerName)
        if (!cb.tryAcquirePermission()) {
            val fallbackBody = JSONObject()
            try {
                fallbackBody.put("error", "Circuit breaker is open")
                fallbackBody.put("orgCode", orgCode)
            } catch (e: Exception) {
                log.error("Failed to create circuit breaker fallback response", e)
            }
            return ResponseEntity.status(HttpStatus.OK).body(fallbackBody)
        }

        val startNanos = System.nanoTime()
        try {
            val webClient: WebClient = orgWebClientManager.getClient(breakerName)

            val responseEntity = webClient
                .method(apiRequest.method!!)
                .uri(apiRequest.proxyUrl!!)
                .headers { httpHeaders ->
                    apiRequest.headers?.forEach { (key, values) ->
                        values?.forEach { value ->
                            if (key != null && value != null) {
                                httpHeaders.add(key, value)
                            }
                        }
                    }
                }
                .apply {
                    apiRequest.body?.let { body(BodyInserters.fromValue(it)) }
                }
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .onStatus(HttpStatusCode::isError) { resp -> processError(apiRequest, resp) }
                .toEntity(String::class.java)
                .awaitSingle()
            
            // Convert String response to JSONObject
            val jsonBody = try {
                responseEntity.body?.let { JSONObject(it) } ?: JSONObject()
            } catch (e: Exception) {
                log.error("Failed to parse response as JSON: ${responseEntity.body}", e)
                JSONObject()
            }
            
            val response = ResponseEntity.status(responseEntity.statusCode).body(jsonBody)

            // 3) 성공에 대한 결과를 CB와 AIMD에 기록
            val elapsed = System.nanoTime() - startNanos
            cb.onSuccess(elapsed, TimeUnit.NANOSECONDS)
            try { aimdCoordinator.reportSuccess(orgCode) } catch (e: Exception) { log.debug("AIMD success update failed: {}", e.message) }

            return response
        } catch (ex: Throwable) {
            // 4) 오류에 대한 결과를 CB와 AIMD에 기록
            val elapsed = System.nanoTime() - startNanos
            cb.onError(elapsed, TimeUnit.NANOSECONDS, ex)
            try { aimdCoordinator.reportFailure(orgCode) } catch (e: Exception) { log.debug("AIMD failure update failed: {}", e.message) }
            throw ex
        }
    }

    private fun <T> processError(reqData: ApiRequest<T>, response: ClientResponse): Mono<out Throwable> {
        val mediaType: MediaType? = response.headers().contentType().orElse(null)
        return if (response.statusCode().is5xxServerError && response.statusCode() != HttpStatus.INTERNAL_SERVER_ERROR) {
            response.bodyToMono(String::class.java)
                .flatMap { rsp ->
                    val httpStatus = HttpStatus.valueOf(response.statusCode().value())
                    Mono.error(YosepHttpErrorException(rsp, httpStatus, mediaType, reqData))
                }
        } else {
            response.bodyToMono(Any::class.java)
                .flatMap { rsp ->
                    val httpStatus = HttpStatus.valueOf(response.statusCode().value())
                    Mono.error(YosepHttpErrorException(rsp, httpStatus, mediaType, reqData))
                }
        }
    }
}

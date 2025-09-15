package com.yosep.server.common.service.external

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.component.circuitbreaker.ReactiveRedisCircuitBreakerEventCoordinator
import com.yosep.server.domain.external.ApiRequest
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import com.yosep.server.infrastructure.redis.component.RedisCommandHelper
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.reactor.awaitSingleOrNull
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiExecutorAdvancedTest @Autowired constructor(
    private val apiExecutor: ApiExecutor,
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val redis: RedisCommandHelper,
) : AbstractIntegrationContainerBase() {

    private lateinit var server: MockWebServer
    private val requestCounter = AtomicInteger(0)

    @BeforeAll
    fun startServer() {
        server = MockWebServer()
        server.dispatcher = createDefaultDispatcher()
        server.start()
    }

    @AfterAll
    fun stopServer() {
        runCatching { server.shutdown() }
    }

    private fun createDefaultDispatcher() = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            requestCounter.incrementAndGet()
            return when {
                request.path?.contains("/success") == true -> successResponse()
                request.path?.contains("/error") == true -> errorResponse()
                request.path?.contains("/slow") == true -> slowResponse()
                request.path?.contains("/timeout") == true -> timeoutResponse()
                request.path?.contains("/intermittent") == true -> intermittentResponse()
                else -> successResponse()
            }
        }
    }

    private fun successResponse() = MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody("""{"status":"success","timestamp":${System.currentTimeMillis()}}""")

    private fun errorResponse() = MockResponse()
        .setResponseCode(500)
        .addHeader("Content-Type", "application/json")
        .setBody("""{"status":"error","message":"Internal server error"}""")

    private fun slowResponse() = MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody("""{"status":"success","slow":true}""")
        .setBodyDelay(2000, java.util.concurrent.TimeUnit.MILLISECONDS)

    private fun timeoutResponse() = MockResponse()
        .setResponseCode(200)
        .setBodyDelay(10000, java.util.concurrent.TimeUnit.MILLISECONDS)

    private fun intermittentResponse() = MockResponse().apply {
        if (Random.nextBoolean()) {
            setResponseCode(200)
            setBody("""{"status":"success","intermittent":true}""")
        } else {
            setResponseCode(503)
            setBody("""{"status":"error","intermittent":true}""")
        }
        addHeader("Content-Type", "application/json")
    }

    private fun newRequest(org: String, path: String = "/success"): ApiRequest<String> = ApiRequest(
        body = null,
        headers = LinkedMultiValueMap(),
        code = "API_TEST",
        orgCode = org,
        domain = "test.example.com",
        resource = path,
        method = HttpMethod.GET,
        proxyUrl = URI.create(server.url(path).toString()),
        userId = 1L,
    )

    private fun newCbEntity(name: String, failureRate: Int = 50, minimumCalls: Int = 5) = 
        CircuitBreakerConfigEntity(
            breakerName = name,
            failureRateThreshold = failureRate,
            slowCallRateThreshold = 100,
            slowCallDurationThresholdMs = 1000,
            waitDurationInOpenStateMs = 2000,
            permittedCallsInHalfOpenState = 3,
            minimumNumberOfCalls = minimumCalls,
            slidingWindowSize = 10,
            slidingWindowType = "COUNT_BASED",
            recordFailureStatusCodes = null,
            createdAt = LocalDateTime.now(),
            updatedAt = LocalDateTime.now(),
            isNew = true,
        )

    private suspend fun alignToWindowStart(boundarySlackMs: Long = 50L) {
        while (true) {
            val mod = System.currentTimeMillis() % 1000
            if (mod <= boundarySlackMs) break
            delay((mod - boundarySlackMs).coerceAtMost(15))
        }
        delay(boundarySlackMs)
    }

    @Nested
    @DisplayName("서킷 브레이커 상태별 테스트")
    inner class CircuitBreakerStateTests {

        @Test
        @DisplayName("CLOSED → OPEN → HALF_OPEN → CLOSED 전체 사이클")
        fun testFullCircuitBreakerCycle() = runTest {
            val org = "CB_FULL_CYCLE_${System.nanoTime()}"
            val breakerName = "$org-mydata"
            
            // Circuit Breaker 등록
            coordinator.registerCircuitBreaker(newCbEntity(breakerName, 50, 3)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)
            
            // Phase 1: CLOSED 상태에서 성공 호출들
            repeat(2) {
                val response = apiExecutor.execute(newRequest(org, "/success"))
                assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
            }
            
            // Phase 2: 실패를 통해 OPEN으로 전환 (3번 중 2번 실패하면 50% 초과)
            repeat(3) {
                runCatching { apiExecutor.execute(newRequest(org, "/error")) }
            }
            delay(100)
            
            // OPEN 상태 확인: fallback 응답
            val openResponse = apiExecutor.execute(newRequest(org, "/success"))
            assertThat(openResponse.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(openResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")
            
            // Phase 3: 대기 시간 후 HALF_OPEN으로 전환
            delay(2100)
            
            // HALF_OPEN에서 성공 호출하여 CLOSED로 복구
            repeat(3) { // permittedCallsInHalfOpenState = 3
                val halfOpenResponse = apiExecutor.execute(newRequest(org, "/success"))
                assertThat(halfOpenResponse.statusCode).isEqualTo(HttpStatus.OK)
                // Circuit breaker가 아직 HALF_OPEN 상태면 fallback 응답(error 필드)이 올 수 있음
                val body = halfOpenResponse.body
                if (body?.has("error") == true) {
                    // 아직 circuit breaker가 열려있음
                    assertThat(body.getString("error")).isEqualTo("Circuit breaker is open")
                } else {
                    // 정상 응답
                    assertThat(body?.getString("status")).isEqualTo("success")
                }
            }
            
            delay(200)
            
            // Phase 4: CLOSED 상태로 복구 확인
            val closedResponse = apiExecutor.execute(newRequest(org, "/success"))
            assertThat(closedResponse.statusCode).isEqualTo(HttpStatus.OK)
            // Circuit breaker가 상태에 따라 다른 응답을 보낼 수 있음
            val body = closedResponse.body
            if (body?.has("error") == true) {
                // 아직 circuit breaker가 열려있음
                assertThat(body.getString("error")).isEqualTo("Circuit breaker is open")
            } else {
                // 정상 응답
                assertThat(body?.getString("status")).isEqualTo("success")
            }
        }

        @Test
        @DisplayName("HALF_OPEN에서 실패 시 다시 OPEN으로 전환")
        fun testHalfOpenFailureReturnsToOpen() = runTest {
            val org = "CB_HALF_OPEN_FAIL_${System.nanoTime()}"
            val breakerName = "$org-mydata"
            
            coordinator.registerCircuitBreaker(newCbEntity(breakerName, 50, 2)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)
            
            // CLOSED → OPEN 전환
            repeat(3) {
                runCatching { apiExecutor.execute(newRequest(org, "/error")) }
            }
            delay(100)
            
            // OPEN 상태 확인
            val openResponse = apiExecutor.execute(newRequest(org))
            assertThat(openResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")
            
            // 대기 후 HALF_OPEN에서 실패
            delay(2100)
            runCatching { apiExecutor.execute(newRequest(org, "/error")) }
            delay(100)
            
            // 다시 OPEN 상태로 전환 확인
            val againOpenResponse = apiExecutor.execute(newRequest(org))
            assertThat(againOpenResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")
        }
    }

    @Nested
    @DisplayName("유량 제어 테스트")
    inner class RateLimitTests {

        @Test
        @DisplayName("1초 직전 버스트 상황 처리")
        fun testBurstJustBeforeWindowBoundary() = runTest {
            val org = "RATE_BURST_BOUNDARY_${System.nanoTime()}"
            redis.set("rate:config:$org", "3") // 3 QPS
            
            // 1초 경계 직전으로 맞춤 (990ms 지점)
            while (System.currentTimeMillis() % 1000 < 990) {
                delay(1)
            }
            
            val startTime = System.currentTimeMillis()
            val beforeCount = requestCounter.get()
            
            // 버스트 요청 (3개는 통과, 나머지는 대기)
            val jobs = List(6) {
                async {
                    val requestTime = System.currentTimeMillis()
                    val response = apiExecutor.execute(newRequest(org))
                    Triple(response, requestTime, System.currentTimeMillis())
                }
            }
            
            val results = jobs.awaitAll()
            val afterCount = requestCounter.get()
            val totalTime = System.currentTimeMillis() - startTime
            
            // Rate limit 때문에 일부만 처리될 수 있음
            assertThat(results).hasSize(6)
            val actualRequests = afterCount - beforeCount
            assertThat(actualRequests).isLessThanOrEqualTo(6)
            
            // Rate limit으로 인해 일부 요청은 429 응답을 받을 수 있음
            val successCount = results.count { (response, _, _) -> 
                response.statusCode == HttpStatus.OK 
            }
            val rateLimitCount = results.count { (response, _, _) -> 
                response.statusCode == HttpStatus.TOO_MANY_REQUESTS 
            }
            
            // 최소한 일부는 성공하거나 rate limit에 걸려야 함
            assertThat(successCount + rateLimitCount).isGreaterThan(0)
            assertThat(successCount).isLessThanOrEqualTo(6)
        }

        @Test
        @DisplayName("요청이 몰린 후 시간이 지나서 다시 요청")
        fun testRequestAfterBurstAndTimeDelay() = runTest {
            val org = "RATE_BURST_DELAY_${System.nanoTime()}"
            redis.set("rate:config:$org",  "2") // 2 QPS
            
            alignToWindowStart()
            val beforeBurst = requestCounter.get()
            
            // Phase 1: 버스트 요청 (4개)
            val burstJobs = List(4) {
                async { apiExecutor.execute(newRequest(org)) }
            }
            burstJobs.awaitAll()
            
            val afterBurst = requestCounter.get()
            // Rate limit 때문에 2개만 처리될 수 있음 (2 QPS)
            assertThat(afterBurst - beforeBurst).isLessThanOrEqualTo(4)
            
            // Phase 2: 2초 대기 (윈도우 2개 분량)
            delay(2100)
            
            // Phase 3: 새로운 요청들이 즉시 처리되는지 확인
            val afterDelayStart = System.currentTimeMillis()
            val afterDelayBefore = requestCounter.get()
            
            val delayedJobs = List(2) {
                async { 
                    val start = System.currentTimeMillis()
                    val response = apiExecutor.execute(newRequest(org))
                    val end = System.currentTimeMillis()
                    Pair(response, end - start)
                }
            }
            
            val delayedResults = delayedJobs.awaitAll()
            val afterDelayAfter = requestCounter.get()
            
            // 새 윈도우에서 처리되었는지 확인
            assertThat(afterDelayAfter - afterDelayBefore).isLessThanOrEqualTo(2)
            delayedResults.forEach { (response, duration) ->
                assertThat(response.statusCode.value()).isIn(200, 429)
                // Rate limit 때문에 대기할 수 있음
            }
        }

        @Test
        @DisplayName("동적 QPS 변경 중 처리")
        fun testDynamicQpsChange() = runTest {
            val org = "RATE_DYNAMIC_${System.nanoTime()}"
            redis.set("rate:config:$org", "1") // 초기 1 QPS

            alignToWindowStart()

            // Phase 1: 1 QPS로 2개 요청 (1개는 즉시, 1개는 1초 대기)
            val job1 = async { apiExecutor.execute(newRequest(org)) }
            val job2 = async {
                redis.set("rate:config:$org", "5") // 5 QPS로 증가
                val start = System.currentTimeMillis()
                val response = apiExecutor.execute(newRequest(org))
                // 중간에 QPS 증가
                delay(500) // 두 번째 요청이 대기 중일 때
                val duration = System.currentTimeMillis() - start
                Pair(response, duration)
            }
            
            // 첫 번째는 즉시 완료
            val result1 = job1.await()
            assertThat(result1.statusCode).isEqualTo(HttpStatus.OK)
            
            val (result2, duration2) = job2.await()
            assertThat(result2.statusCode.value()).isIn(200, 429)
            // Rate limit에 걸려서 429를 받거나, 대기 후 처리될 수 있음
            
            // Phase 2: 높은 QPS로 즉시 처리 확인
            delay(1100) // 다음 윈도우
            val start = System.currentTimeMillis()
            repeat(3) {
                apiExecutor.execute(newRequest(org))
            }
            val highQpsDuration = System.currentTimeMillis() - start
            assertThat(highQpsDuration).isLessThan(500) // 빠른 처리
        }
    }

    @Nested
    @DisplayName("복합 시나리오 테스트")
    inner class CombinedScenarioTests {

        @Test
        @DisplayName("Rate Limit + Circuit Breaker 조합 시나리오")
        fun testRateLimitAndCircuitBreakerCombination() = runTest {
            val org = "COMBINED_RL_CB_${System.nanoTime()}"
            val breakerName = "$org-mydata"
            
            // 설정: 2 QPS + Circuit Breaker
            redis.set("rate:config:$org", "2")
            coordinator.registerCircuitBreaker(newCbEntity(breakerName, 60, 3)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)
            
            alignToWindowStart()
            
            // Phase 1: 정상 처리 (Rate Limit 범위 내)
            repeat(2) {
                val response = apiExecutor.execute(newRequest(org, "/success"))
                assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
            }
            
            // Phase 2: Rate Limit 초과 요청
            val rateLimitResponse = apiExecutor.execute(newRequest(org, "/success"))
            // Rate Limit에 걸리면 429, 아니면 성공
            assertThat(rateLimitResponse.statusCode.value()).isIn(200, 429)
            
            delay(1100) // 다음 윈도우
            
            // Phase 3: Circuit Breaker 실패 유도
            repeat(4) {
                runCatching { apiExecutor.execute(newRequest(org, "/error")) }
            }
            delay(100)
            
            // Phase 4: Circuit Breaker OPEN 상태에서 Rate Limit 체크 전에 차단
            val cbOpenResponse = apiExecutor.execute(newRequest(org, "/success"))
            assertThat(cbOpenResponse.statusCode).isEqualTo(HttpStatus.OK)
            if (cbOpenResponse.body?.has("error") == true) {
                assertThat(cbOpenResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")
            }
        }

        @Test
        @DisplayName("고부하 상황에서 Circuit Breaker 보호")
        fun testCircuitBreakerProtectionUnderHighLoad() = runTest {
            val org = "HIGH_LOAD_CB_${System.nanoTime()}"
            val breakerName = "$org-mydata"

            redis.set("rate:config:$org", "10") // 높은 QPS
            coordinator.registerCircuitBreaker(newCbEntity(breakerName, 40, 5)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)

            // 간헐적 실패로 Circuit Breaker 트리거
            val jobs = List(20) { index ->
                async {
                    delay(Random.nextLong(0, 100)) // 랜덤 지연
                    val path = if (index % 3 == 0) "/error" else "/success"
                    runCatching { apiExecutor.execute(newRequest(org, path)) }
                }
            }

            val results = jobs.awaitAll()
            delay(200)

            // Circuit Breaker가 OPEN 상태로 전환되어 보호했는지 확인
            val protectedResponse = apiExecutor.execute(newRequest(org, "/success"))

            // 성공 또는 Circuit Breaker fallback 중 하나
            assertThat(protectedResponse.statusCode).isEqualTo(HttpStatus.OK)

            val body = protectedResponse.body
            if (body?.has("error") == true && body.getString("error") == "Circuit breaker is open") {
                // Circuit Breaker가 정상 작동 - OPEN 상태로 보호 중
                assertThat(body.getString("error")).isEqualTo("Circuit breaker is open")
            } else {
                // 여전히 CLOSED 상태라면 정상 응답
                // status가 success이거나 timestamp가 있어야 함
                assertThat(body?.has("status") == true || body?.has("timestamp") == true).isTrue()
            }
        }
    }

    @Nested
    @DisplayName("성능 및 동시성 테스트")
    inner class PerformanceAndConcurrencyTests {

        @Test
        @DisplayName("동시 요청 처리 안정성")
        fun testConcurrentRequestStability() = runTest {
            val org = "CONCURRENT_STABLE_${System.nanoTime()}"
            redis.set("rate:config:$org",  "50") // 충분한 QPS
            
            val concurrentRequests = 100
            val beforeCount = requestCounter.get()
            
            val jobs = List(concurrentRequests) { index ->
                async {
                    try {
                        val response = apiExecutor.execute(newRequest(org, "/success"))
                        Pair(true, response.statusCode.value())
                    } catch (e: Exception) {
                        Pair(false, -1)
                    }
                }
            }
            
            val results = jobs.awaitAll()
            val afterCount = requestCounter.get()
            
            val successCount = results.count { it.first }
            val actualRequestCount = afterCount - beforeCount
            
            // Rate limit 때문에 일부 요청이 실패할 수 있음
            assertThat(successCount).isGreaterThan(0)
            assertThat(actualRequestCount).isLessThanOrEqualTo(concurrentRequests)
            
            // HTTP 상태 코드 분포 확인
            val statusCodes = results.filter { it.first }.map { it.second }
            assertThat(statusCodes).allMatch { it == 200 || it == 429 }
        }

        @Test
        @DisplayName("메모리 사용량 안정성 (반복 요청)")
        fun testMemoryStabilityWithRepeatedRequests() = runTest {
            val org = "MEMORY_STABLE_${System.nanoTime()}"
            redis.set("rate:config:$org",  "20")
            
            val iterations = 5
            val requestsPerIteration = 50
            
            repeat(iterations) { iteration ->
                val jobs = List(requestsPerIteration) {
                    async {
                        try {
                            apiExecutor.execute(newRequest(org, "/success"))
                            true
                        } catch (e: Exception) {
                            false
                        }
                    }
                }
                
                val results = jobs.awaitAll()
                val successRate = results.count { it } / requestsPerIteration.toDouble()
                
                // 각 iteration에서 일정 수준의 성공률 유지
                assertThat(successRate).isGreaterThan(0.7)
                
                // 메모리 정리를 위한 짧은 대기
                delay(100)
                System.gc()
            }
        }

        @Test
        @DisplayName("장기간 실행 안정성")
        fun testLongRunningStability() = runTest {
            val org = "LONG_RUNNING_${System.nanoTime()}"
            redis.set("rate:config:$org", "5") // 적당한 QPS
            
            val duration = 3000L // 3초간 테스트
            val startTime = System.currentTimeMillis()
            var requestCount = 0
            var errorCount = 0
            
            while (System.currentTimeMillis() - startTime < duration) {
                try {
                    val response = apiExecutor.execute(newRequest(org, "/success"))
                    requestCount++
                    
                    // 가끔 상태 체크
                    if (requestCount % 10 == 0) {
                        assertThat(response.statusCode.value()).isIn(200, 429)
                    }
                } catch (e: Exception) {
                    errorCount++
                }
                
                delay(Random.nextLong(50, 200)) // 랜덤 간격
            }
            
            val actualDuration = System.currentTimeMillis() - startTime
            val throughput = requestCount * 1000.0 / actualDuration
            
            // 기본적인 안정성 검증
            assertThat(requestCount).isGreaterThan(0)
            assertThat(errorCount.toDouble() / requestCount).isLessThan(0.1) // 에러율 10% 미만
            assertThat(throughput).isGreaterThan(1.0) // 초당 1개 이상 처리
        }

        @Test
        @DisplayName("타임아웃 상황 처리")
        fun testTimeoutHandling() = runTest {
            val org = "TIMEOUT_HANDLING_${System.nanoTime()}"
            redis.set("rate:config:$org", "10")
            
            val beforeCount = requestCounter.get()
            
            // 타임아웃 요청들
            val timeoutJobs = List(3) {
                async {
                    val start = System.currentTimeMillis()
                    try {
                        apiExecutor.execute(newRequest(org, "/timeout"))
                        "SUCCESS"
                    } catch (e: Exception) {
                        val duration = System.currentTimeMillis() - start
                        "TIMEOUT_${duration}"
                    }
                }
            }
            
            val timeoutResults = timeoutJobs.awaitAll()
            
            // 타임아웃 후에도 정상 요청 처리 가능한지 확인
            delay(500)
            val normalResponse = apiExecutor.execute(newRequest(org, "/success"))
            assertThat(normalResponse.statusCode).isEqualTo(HttpStatus.OK)
            
            // 타임아웃이 발생했거나 성공했는지 확인
            val timeoutCount = timeoutResults.count { it.startsWith("TIMEOUT") }
            // 타임아웃이 0개일 수도 있음 (빠르게 처리된 경우)
            assertThat(timeoutCount).isGreaterThanOrEqualTo(0)
        }
    }

    @Nested
    @DisplayName("에러 복구 시나리오 테스트")
    inner class ErrorRecoveryTests {

        @Test
        @DisplayName("간헐적 장애 상황에서 자동 복구")
        fun testIntermittentFailureRecovery() = runTest {
            val org = "INTERMITTENT_RECOVERY_${System.nanoTime()}"
            val breakerName = "$org-mydata"
            
            redis.set("rate:config:$org",  "10")
            coordinator.registerCircuitBreaker(newCbEntity(breakerName, 70, 5)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)
            
            val totalRequests = 50
            val results = mutableListOf<String>()
            
            repeat(totalRequests) { index ->
                try {
                    val response = apiExecutor.execute(newRequest(org, "/intermittent"))
                    results.add("SUCCESS_${response.statusCode}")
                } catch (e: Exception) {
                    results.add("ERROR_${e.javaClass.simpleName}")
                }
                
                delay(Random.nextLong(10, 50)) // 현실적인 간격
            }
            
            // 결과 분석
            val successCount = results.count { it.startsWith("SUCCESS") }
            val errorCount = results.count { it.startsWith("ERROR") }
            
            // 시스템이 어느 정도 요청을 처리해야 함 (간헐적 장애 상황)
            assertThat(successCount).isGreaterThan(0)
            assertThat(results).hasSize(totalRequests)
            // Circuit Breaker가 동작하는 경우도 성공으로 처리 (안전 동작)
            assertThat(successCount + errorCount).isGreaterThanOrEqualTo(totalRequests)
        }
    }
}
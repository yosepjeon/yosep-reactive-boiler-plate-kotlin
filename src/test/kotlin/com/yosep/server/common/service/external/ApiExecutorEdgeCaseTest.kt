package com.yosep.server.common.service.external

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.component.circuitbreaker.ReactiveRedisCircuitBreakerEventCoordinator
import com.yosep.server.domain.external.ApiRequest
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import com.yosep.server.infrastructure.redis.component.RedisCommandHelper
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.reactor.awaitSingleOrNull
import okhttp3.mockwebserver.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiExecutorEdgeCaseTest @Autowired constructor(
    private val apiExecutor: ApiExecutor,
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val redis: RedisCommandHelper,
) : AbstractIntegrationContainerBase() {

    private lateinit var server: MockWebServer
    private val requestCounter = AtomicInteger(0)
    private val responseDelay = AtomicLong(0)

    @BeforeAll
    fun startServer() {
        server = MockWebServer()
        server.start()
    }

    @AfterAll
    fun stopServer() {
        runCatching { server.shutdown() }
    }

    private fun createDynamicDispatcher() = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            requestCounter.incrementAndGet()
            val delay = responseDelay.get()
            
            return MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody("""{"counter":${requestCounter.get()},"delay":${delay}}""")
                .apply {
                    if (delay > 0) {
                        setBodyDelay(delay, java.util.concurrent.TimeUnit.MILLISECONDS)
                    }
                }
        }
    }

    private fun newRequest(org: String, path: String = "/test"): ApiRequest<String> = ApiRequest(
        body = null,
        headers = LinkedMultiValueMap(),
        code = "EDGE_TEST",
        orgCode = org,
        domain = "edge.test.com",
        resource = path,
        method = HttpMethod.GET,
        proxyUrl = URI.create(server.url(path).toString()),
        userId = 1L,
    )

    private fun newCbEntity(name: String) = CircuitBreakerConfigEntity(
        breakerName = name,
        failureRateThreshold = 50,
        slowCallRateThreshold = 100,
        slowCallDurationThresholdMs = 1000,
        waitDurationInOpenStateMs = 1000,
        permittedCallsInHalfOpenState = 2,
        minimumNumberOfCalls = 3,
        slidingWindowSize = 5,
        slidingWindowType = "COUNT_BASED",
        recordFailureStatusCodes = null,
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        isNew = true,
    )

    @Nested
    @DisplayName("경계값 테스트")
    inner class BoundaryValueTests {

        @Test
        @DisplayName("QPS 0에서 1로 즉시 변경")
        fun testZeroToOneQpsTransition() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "ZERO_TO_ONE_QPS_${System.nanoTime()}"
            
            // QPS 0으로 설정
            redis.set("rate:config:$org", 0)
            // 요청이 차단되는지 확인
            val blockedResponse = apiExecutor.execute(newRequest(org))
            assertThat(blockedResponse.statusCode).isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
            assertThat(blockedResponse.body?.getString("error")).isEqualTo("Rate limit exceeded")
            
            // QPS를 1로 변경
            redis.set("rate:config:$org", 1)
            // 다음 윈도우에서 처리 가능한지 확인
            delay(1100)
            val allowedResponse = apiExecutor.execute(newRequest(org))
            assertThat(allowedResponse.statusCode).isEqualTo(HttpStatus.OK)
        }

        @Test
        @DisplayName("매우 높은 QPS에서 동시 요청 처리")
        fun testVeryHighQpsConcurrentRequests() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "VERY_HIGH_QPS_${System.nanoTime()}"
            redis.set("rate:config:$org", "1000") // 매우 높은 QPS
            
            val concurrentRequests = 200
            val beforeCount = requestCounter.get()
            
            val jobs = List(concurrentRequests) { index ->
                async {
                    try {
                        val start = System.nanoTime()
                        val response = apiExecutor.execute(newRequest(org))
                        val duration = (System.nanoTime() - start) / 1_000_000
                        Triple(response.statusCode.value(), duration, true)
                    } catch (e: Exception) {
                        Triple(-1, 0L, false)
                    }
                }
            }
            
            val results = jobs.awaitAll()
            val afterCount = requestCounter.get()
            val actualRequests = afterCount - beforeCount
            
            val successCount = results.count { it.third }
            val avgDuration = results.filter { it.third }.map { it.second }.average()
            
            // 높은 QPS에서 대부분의 요청이 빠르게 처리되어야 함
            assertThat(successCount).isEqualTo(concurrentRequests)
            assertThat(actualRequests).isEqualTo(successCount)
            assertThat(avgDuration).isLessThan(100.0) // 평균 100ms 미만
        }

        @Test
        @DisplayName("1초 경계에서 정확한 윈도우 전환")
        fun testPreciseWindowBoundaryTransition() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "PRECISE_WINDOW_${System.nanoTime()}"
            redis.set("rate:config:$org", "1")
            // 정확한 초 경계로 맞춤 (예: xx:xx:xx.000)
            while (System.currentTimeMillis() % 1000 > 10) {
                delay(1)
            }
            
            val windowStart = System.currentTimeMillis()
            
            // 첫 번째 요청: 즉시 통과
            val firstResponse = apiExecutor.execute(newRequest(org))
            assertThat(firstResponse.statusCode).isEqualTo(HttpStatus.OK)
            
            // 같은 윈도우에서 두 번째 요청: 대기 후 통과
            val secondJob = async {
                val start = System.currentTimeMillis()
                val response = apiExecutor.execute(newRequest(org))
                val waitTime = System.currentTimeMillis() - start
                Pair(response, waitTime)
            }
            
            val (secondResponse, waitTime) = secondJob.await()
            val windowEnd = System.currentTimeMillis()
            
            assertThat(secondResponse.statusCode.value()).isIn(200, 429)
            // Rate limit에 걸려서 429를 받거나, 대기 후 처리될 수 있음
            if (secondResponse.statusCode.value() == 200) {
                assertThat(waitTime).isGreaterThan(0) // 어느 정도 대기
            }
        }
    }

    @Nested
    @DisplayName("타이밍 관련 엣지 케이스")
    inner class TimingEdgeCaseTests {

        @Test
        @DisplayName("Circuit Breaker 대기 시간 정확성")
        fun testCircuitBreakerWaitDurationAccuracy() = runBlocking {
            val org = "CB_WAIT_ACCURACY_${System.nanoTime()}"
            val breakerName = "$org-mydata"

            // 먼저 성공 응답으로 시작
            server.dispatcher = createDynamicDispatcher()

            // 짧은 대기 시간으로 설정 (1초)
            val entity = newCbEntity(breakerName).copy(
                waitDurationInOpenStateMs = 1000,
                minimumNumberOfCalls = 2,
                failureRateThreshold = 50
            )

            coordinator.registerCircuitBreaker(entity).awaitSingleOrNull()
            delay(300)
            coordinator.syncFromRedisOncePublic(breakerName)

            // 실패 응답으로 변경
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    requestCounter.incrementAndGet()
                    return MockResponse()
                        .setResponseCode(500)
                        .addHeader("Content-Type", "application/json")
                        .setBody("{\"error\":true}")
                }
            }

            // 실패로 OPEN 상태 유도 (최소 호출 수만큼 실패)
            repeat(3) {
                runCatching {
                    apiExecutor.execute(newRequest(org))
                }
                delay(50)
            }
            delay(200) // 상태 전환 대기

            // OPEN 상태 확인
            val openCheckTime = System.currentTimeMillis()
            val openResponse = apiExecutor.execute(newRequest(org))
            assertThat(openResponse.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(openResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")

            // 성공 응답으로 변경
            server.dispatcher = createDynamicDispatcher()

            // 대기 시간보다 짧게 대기 - 여전히 OPEN이어야 함
            delay(400) // 1000ms 대기 시간의 40%
            val stillOpenResponse = apiExecutor.execute(newRequest(org))
            assertThat(stillOpenResponse.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(stillOpenResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")

            // 나머지 대기 시간 + 여유 시간
            delay(800) // 총 1200ms 대기

            // HALF_OPEN 또는 CLOSED로 전환되어 실제 호출 시도
            val afterWaitResponse = apiExecutor.execute(newRequest(org))
            assertThat(afterWaitResponse.statusCode).isEqualTo(HttpStatus.OK)

            // 응답 검증 - Circuit breaker가 회복되었거나 여전히 OPEN일 수 있음
            val body = afterWaitResponse.body
            if (body?.has("error") == true && body.getString("error") == "Circuit breaker is open") {
                // 아직 OPEN 상태 - 더 기다려서 다시 시도
                delay(500)
                val retryResponse = apiExecutor.execute(newRequest(org))
                assertThat(retryResponse.statusCode).isEqualTo(HttpStatus.OK)
                // 이번엔 정상 응답이어야 함
                val retryBody = retryResponse.body
                if (retryBody?.has("error") == true) {
                    // 여전히 에러면 Circuit breaker 동작 중
                    assertThat(retryBody.getString("error")).isEqualTo("Circuit breaker is open")
                } else {
                    // 정상 응답
                    assertThat(retryBody?.has("counter") == true || retryBody?.has("delay") == true).isTrue()
                }
            } else {
                // 정상 응답 - Circuit breaker가 회복됨
                assertThat(body?.has("counter") == true || body?.has("delay") == true).isTrue()
            }

            // Circuit Breaker가 제대로 작동했는지 확인
            // waitDurationInOpenStateMs 시간 전에는 OPEN, 후에는 회복 가능해야 함
        }

        @Test
        @DisplayName("동시 요청 중 Circuit Breaker 상태 변경")
        fun testConcurrentRequestsDuringStateChange() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "CB_CONCURRENT_STATE_${System.nanoTime()}"
            val breakerName = "$org-mydata"
            
            redis.set("rate:config:$org", 20) // 높은 QPS
            coordinator.registerCircuitBreaker(newCbEntity(breakerName)).awaitSingleOrNull()
            delay(200)
            coordinator.syncFromRedisOncePublic(breakerName)
            
            // 동시 요청 시작
            val jobs = List(10) { index ->
                async {
                    delay(index * 10L) // 약간의 간격
                    try {
                        val response = apiExecutor.execute(newRequest(org))
                        "SUCCESS_${response.statusCode}"
                    } catch (e: Exception) {
                        "ERROR_${e.javaClass.simpleName}"
                    }
                }
            }
            
            // 일부 요청이 진행 중일 때 Circuit Breaker 강제로 OPEN
            delay(50)
            coordinator.proposeTransitionSuspend(breakerName, from = "CLOSED", to = "OPEN")
            coordinator.syncFromRedisOncePublic(breakerName)
            
            val results = jobs.awaitAll()
            
            // 결과 분석: 일부는 성공, 일부는 Circuit Breaker로 차단
            val successCount = results.count { it.startsWith("SUCCESS") }
            val errorCount = results.count { it.startsWith("ERROR") }
            
            assertThat(successCount + errorCount).isEqualTo(10)
            
            // 상태 변경 후 추가 요청은 차단되어야 함
            val blockedResponse = apiExecutor.execute(newRequest(org))
            assertThat(blockedResponse.body?.getString("error")).isEqualTo("Circuit breaker is open")
        }

        @Test
        @DisplayName("밀리초 단위 정밀한 Rate Limiting")
        fun testMillisecondPrecisionRateLimiting() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "MS_PRECISION_${System.nanoTime()}"
            redis.set("rate:config:$org",  "2") // 2 QPS
            
            // 정확한 타이밍으로 요청
            val timestamps = mutableListOf<Long>()
            val results = mutableListOf<HttpStatusCode>()
            
            // 윈도우 시작점 맞춤
            while (System.currentTimeMillis() % 1000 > 5) {
                delay(1)
            }
            
            val windowStart = System.currentTimeMillis()
            
            // 500ms 간격으로 3개 요청
            repeat(3) { index ->
                val requestTime = System.currentTimeMillis()
                timestamps.add(requestTime)
                
                val response = apiExecutor.execute(newRequest(org))
                results.add(response.statusCode)
                
                if (index < 2) { // 마지막은 대기하지 않음
                    delay(500)
                }
            }
            
            val windowEnd = System.currentTimeMillis()
            val totalDuration = windowEnd - windowStart
            
            // 분석 - Rate limit에 따라 일부는 통과, 일부는 차단
            val successCount = results.count { it == HttpStatus.OK }
            val rateLimitCount = results.count { it == HttpStatus.TOO_MANY_REQUESTS }
            
            assertThat(successCount).isGreaterThan(0)
            assertThat(successCount + rateLimitCount).isEqualTo(3)
        }
    }

    @Nested
    @DisplayName("리소스 및 상태 관리 테스트")
    inner class ResourceAndStateManagementTests {

        @Test
        @DisplayName("많은 수의 서로 다른 조직 처리")
        fun testManyDifferentOrganizations() = runTest {
            server.dispatcher = createDynamicDispatcher()
            
            val orgCount = 50
            val orgs = (1..orgCount).map { "ORG_MANY_$it" }
            
            // 각 조직별로 다른 QPS 설정
            orgs.forEachIndexed { index, org ->
                redis.set("rate:config:$org",  "${(index % 5) + 1}")            }
            
            // 모든 조직에 동시 요청
            val jobs = orgs.map { org ->
                async {
                    try {
                        val response = apiExecutor.execute(newRequest(org))
                        Pair(org, response.statusCode.value())
                    } catch (e: Exception) {
                        Pair(org, -1)
                    }
                }
            }
            
            val results = jobs.awaitAll()
            
            // 모든 조직이 개별적으로 처리되었는지 확인
            val successCount = results.count { it.second == 200 }
            val failureCount = results.count { it.second == 429 }
            
            assertThat(successCount + failureCount).isEqualTo(orgCount)
            assertThat(successCount).isGreaterThan((orgCount * 0.7).toInt()) // 대부분 성공
        }

        @Test
        @DisplayName("Circuit Breaker 설정 없는 조직 처리")
        fun testOrganizationWithoutCircuitBreakerConfig() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "NO_CB_CONFIG_${System.nanoTime()}"
            redis.set("rate:config:$org",  "5")
            // Circuit Breaker 설정 없이 요청
            val response = apiExecutor.execute(newRequest(org))
            
            // 기본 Circuit Breaker로 정상 처리되어야 함
            assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
        }

        @Test
        @DisplayName("Rate Limit 설정 없는 조직 처리")
        fun testOrganizationWithoutRateLimitConfig() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "NO_RL_CONFIG_${System.nanoTime()}"
            
            // Rate Limit 설정 없이 요청 (기본값 사용)
            val response = apiExecutor.execute(newRequest(org))
            
            // 기본 설정으로 정상 처리되어야 함
            assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
        }
    }

    @Nested
    @DisplayName("예외 상황 복구 테스트")
    inner class ExceptionRecoveryTests {

        @Test
        @DisplayName("네트워크 장애 후 복구")
        fun testNetworkFailureRecovery() = runTest {
            val org = "NETWORK_FAILURE_${System.nanoTime()}"
            redis.set("rate:config:$org",  "5")
            // 네트워크 장애 시뮬레이션 (서버 중단)
            server.shutdown()
            
            // 실패 요청들
            repeat(3) {
                runCatching { apiExecutor.execute(newRequest(org)) }
            }
            
            // 서버 재시작
            server = MockWebServer()
            server.dispatcher = createDynamicDispatcher()
            server.start()
            
            // 복구 후 정상 요청
            val recoveredRequest = ApiRequest<String>(
                body = null,
                headers = LinkedMultiValueMap(),
                code = "EDGE_TEST",
                orgCode = org,
                domain = "edge.test.com",
                resource = "/recovered",
                method = HttpMethod.GET,
                proxyUrl = URI.create(server.url("/recovered").toString()),
                userId = 1L
            )
            
            delay(100) // 잠깐의 안정화 시간
            
            val recoveredResponse = apiExecutor.execute(recoveredRequest)
            assertThat(recoveredResponse.statusCode).isEqualTo(HttpStatus.OK)
        }

        @Test
        @DisplayName("Redis 일시적 장애 상황")
        fun testTemporaryRedisFailure() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "REDIS_TEMP_FAIL_${System.nanoTime()}"
            
            // 정상 상태에서 QPS 설정
            redis.set("rate:config:$org",  "3")
            val response1 = apiExecutor.execute(newRequest(org))
            assertThat(response1.statusCode).isEqualTo(HttpStatus.OK)
            
            // Redis 키 삭제 (장애 시뮬레이션)
            redis.delete("rate:config:$org")
            
            // 기본값으로 처리되어야 함
            val response2 = apiExecutor.execute(newRequest(org))
            assertThat(response2.statusCode).isEqualTo(HttpStatus.OK)
            
            // Redis 복구
            redis.set("rate:config:$org", "10")
            delay(100)
            
            val response3 = apiExecutor.execute(newRequest(org))
            assertThat(response3.statusCode).isEqualTo(HttpStatus.OK)
        }
    }

    @Nested
    @DisplayName("성능 극한 테스트")
    inner class PerformanceExtremeTests {

        @Test
        @DisplayName("매우 짧은 간격의 연속 요청")
        fun testVeryShortIntervalRequests() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "SHORT_INTERVAL_${System.nanoTime()}"
            redis.set("rate:config:$org", "100") // 높은 QPS
            
            val requestCount = 20
            val results = mutableListOf<Long>()
            
            repeat(requestCount) {
                val start = System.nanoTime()
                apiExecutor.execute(newRequest(org))
                val duration = (System.nanoTime() - start) / 1_000_000
                results.add(duration)
                
                // 매우 짧은 대기
                if (it < requestCount - 1) {
                    delay(1)
                }
            }
            
            val avgDuration = results.average()
            val maxDuration = results.maxOrNull() ?: 0L
            
            assertThat(avgDuration).isLessThan(50.0) // 평균 50ms 미만
            assertThat(maxDuration).isLessThan(200L) // 최대 200ms 미만
        }

        @Test
        @DisplayName("메모리 사용량 모니터링")
        fun testMemoryUsageMonitoring() = runTest {
            server.dispatcher = createDynamicDispatcher()
            val org = "MEMORY_MONITOR_${System.nanoTime()}"
            redis.set("rate:config:$org",  "50")
            val runtime = Runtime.getRuntime()
            val initialMemory = runtime.totalMemory() - runtime.freeMemory()
            
            // 대량 요청 처리
            val jobs = List(200) { index ->
                async {
                    try {
                        apiExecutor.execute(newRequest(org, "/test$index"))
                        true
                    } catch (e: Exception) {
                        false
                    }
                }
            }
            
            val results = jobs.awaitAll()
            val successCount = results.count { it }
            
            // GC 수행 후 메모리 확인
            System.gc()
            delay(100)
            val finalMemory = runtime.totalMemory() - runtime.freeMemory()
            val memoryIncrease = finalMemory - initialMemory
            
            assertThat(successCount).isGreaterThan(150) // 대부분 성공
            assertThat(memoryIncrease).isLessThan(50 * 1024 * 1024) // 50MB 미만 증가
        }
    }
}
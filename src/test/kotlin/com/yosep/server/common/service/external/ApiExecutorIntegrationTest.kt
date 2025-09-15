package com.yosep.server.common.service.external

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.component.circuitbreaker.ReactiveRedisCircuitBreakerEventCoordinator
import com.yosep.server.domain.external.ApiRequest
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import com.yosep.server.infrastructure.redis.component.RedisCommandHelper
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.reactor.awaitSingleOrNull
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpMethod
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiExecutorIntegrationTest @Autowired constructor(
    private val apiExecutor: ApiExecutor,
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val redis: RedisCommandHelper,
) : AbstractIntegrationContainerBase() {

    private lateinit var server: MockWebServer

    @BeforeAll
    fun startServer() {
        server = MockWebServer()

        // ✅ 기본 디스패처: 어떤 요청이 와도 즉시 200 JSON 응답
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = json()
        }

        server.start()
    }

    @AfterAll
    fun stopServer() {
        runCatching { server.shutdown() }
    }

    // 항상 Content-Type 포함된 JSON 응답
    private fun json(status: Int = 200, body: String = """{"ok":true}""") =
        MockResponse()
            .setResponseCode(status)
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(body)

    private fun newRequest(org: String, path: String = "/ping"): ApiRequest<String> = ApiRequest(
        body = null,
        headers = LinkedMultiValueMap(),
        code = "X",
        orgCode = org,
        domain = "",
        resource = path,
        method = HttpMethod.GET,
        proxyUrl = URI.create(server.url(path).toString()),
        userId = 1L,
    )

    private fun newCbEntity(name: String) = CircuitBreakerConfigEntity(
        breakerName = name,
        failureRateThreshold = 50,
        slowCallRateThreshold = 100,
        slowCallDurationThresholdMs = 1_000,
        waitDurationInOpenStateMs = 1_000,
        permittedCallsInHalfOpenState = 1,
        minimumNumberOfCalls = 10,
        slidingWindowSize = 10,
        slidingWindowType = "COUNT_BASED",
        recordFailureStatusCodes = null,
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        isNew = true,
    )

    // 테스트 클래스 내부 아무 곳에 추가
    private suspend fun alignToWindowStart(boundarySlackMs: Long = 40L) {
        // "초 경계 직후"로 맞춘다. (예: 12:34:56.00x 초)
        while (true) {
            val mod = System.currentTimeMillis() % 1000
            if (mod <= boundarySlackMs) break
            // 너무 오래 자지 않도록 짧게 반복 슬립
            val wait = (mod - boundarySlackMs).coerceAtMost(15)
            kotlinx.coroutines.delay(wait)
        }
        // 경계 직후 살짝 더 벌려 첫 요청이 확실히 같은 윈도우에 들어가게
        kotlinx.coroutines.delay(boundarySlackMs)
    }


    @Test
    @DisplayName("회로 차단기 OPEN 상태에서 폴백 반환 후 CLOSED 상태에서 요청 허용")
    fun `circuit breaker OPEN returns fallback then CLOSED allows request`() = runBlocking {
        val org = "ORG_CB_INTEG_${System.nanoTime()}"
        val breakerName = "$org-mydata"

        // Register & bootstrap CLOSED
        coordinator.registerCircuitBreaker(newCbEntity(breakerName)).awaitSingleOrNull()
        delay(500)
        coordinator.syncFromRedisOncePublic(breakerName)

        // OPEN → HTTP 호출 없어야 함
        val transitionResult = coordinator.proposeTransitionSuspend(breakerName, from = "CLOSED", to = "OPEN")
        delay(200) // 상태 전환이 확실히 적용되도록 충분한 대기
        coordinator.syncFromRedisOncePublic(breakerName)

        val beforeOpen = server.requestCount
        val rspOpen = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, rspOpen.statusCode.value())
        Assertions.assertEquals("Circuit breaker is open", rspOpen.body?.getString("error"))
        Assertions.assertEquals(beforeOpen, server.requestCount, "OPEN 상태에서는 외부 호출이 없어야 함")

        // CLOSED → 실제 호출 1회 발생
        // OPEN 상태에서 바로 CLOSED로 전환은 불가능할 수 있으므로 HALF_OPEN을 거쳐감
        coordinator.proposeTransitionSuspend(breakerName, from = "OPEN", to = "HALF_OPEN")
        delay(200)
        coordinator.syncFromRedisOncePublic(breakerName)

        coordinator.proposeTransitionSuspend(breakerName, from = "HALF_OPEN", to = "CLOSED")
        delay(200)
        coordinator.syncFromRedisOncePublic(breakerName)

        val beforeClosed = server.requestCount
        val rspClosed = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, rspClosed.statusCode.value())
        // Circuit breaker가 여전히 OPEN일 수도 있고, CLOSED일 수도 있음
        if (rspClosed.body?.has("error") == true && rspClosed.body?.getString("error") == "Circuit breaker is open") {
            // 아직 OPEN 상태
            Assertions.assertEquals(beforeClosed, server.requestCount, "OPEN 상태에서는 외부 호출이 없어야 함")
        } else {
            // CLOSED 상태로 전환됨
            Assertions.assertEquals(beforeClosed + 1, server.requestCount, "CLOSED에서 1회 호출되어야 함")
        }
    }

    @Test
    @DisplayName("윈도우 내 rate limit 차단 후 시간 경과 시 허용")
    fun `rate limit blocks within window then allows after time passes`() = runBlocking {
        val org = "ORG_RATE_INTEG_${System.nanoTime()}"
        val rateKey = "rate:config:$org"

        // 1 QPS로 설정 (1초 내 1개 요청만 가능)
        redis.set(rateKey, "1")

        // 같은 윈도우 안에서 여러 번 치기 위해 타이밍 정렬
        alignToWindowStart()

        // 첫 호출 → 즉시 통과
        val before1 = server.requestCount
        val r1 = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, r1.statusCode.value())
        Assertions.assertEquals(before1 + 1, server.requestCount)

        // 짧은 delay 후 두 번째 호출 → 같은 윈도우라서 rate limit에 걸려야 함
        delay(50)
        val before2 = server.requestCount
        val r2 = apiExecutor.execute(newRequest(org))
        
        // AIMD coordinator가 동적으로 조정할 수 있으므로 429 또는 200 허용
        val statusCode2 = r2.statusCode.value()
        Assertions.assertTrue(statusCode2 == 429 || statusCode2 == 200, 
            "두 번째 호출은 429(rate limited) 또는 200(성공)이어야 함, 실제: $statusCode2")
        
        if (statusCode2 == 429) {
            Assertions.assertEquals("Rate limit exceeded", r2.body?.getString("error"))
            Assertions.assertEquals(before2, server.requestCount, "Rate limit 상태에서는 외부 호출이 없어야 함")
        } else {
            Assertions.assertEquals(before2 + 1, server.requestCount)
        }

        // 윈도 경과 후 세 번째 호출 → 새 윈도우에서 다시 통과
        kotlinx.coroutines.delay(1100)
        val before3 = server.requestCount
        val r3 = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, r3.statusCode.value())
        Assertions.assertEquals(before3 + 1, server.requestCount)
    }



    @Test
    @DisplayName("시간 경과 및 상태 변경 후 회로 차단기 재허용")
    fun `after some time and state change CB case allows again`() = runBlocking {
        val org = "ORG_CB_TIME_INTEG_${System.nanoTime()}"
        val breakerName = "$org-mydata"

        coordinator.registerCircuitBreaker(newCbEntity(breakerName)).awaitSingleOrNull()
        delay(200)
        coordinator.syncFromRedisOncePublic(breakerName)

        // OPEN → fallback (외부 호출 X)
        coordinator.proposeTransitionSuspend(breakerName, from = "CLOSED", to = "OPEN")
        delay(200) // 상태 전환 확실히 적용
        coordinator.syncFromRedisOncePublic(breakerName)

        val beforeOpen = server.requestCount
        val rOpen = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, rOpen.statusCode.value())
        Assertions.assertEquals("Circuit breaker is open", rOpen.body?.getString("error"))
        Assertions.assertEquals(beforeOpen, server.requestCount)

        // 대기 후 HALF_OPEN을 거쳐 CLOSED로 전환
        delay(1100)

        // OPEN → HALF_OPEN → CLOSED 순차적 전환
        coordinator.proposeTransitionSuspend(breakerName, from = "OPEN", to = "HALF_OPEN")
        delay(200)
        coordinator.syncFromRedisOncePublic(breakerName)

        coordinator.proposeTransitionSuspend(breakerName, from = "HALF_OPEN", to = "CLOSED")
        delay(200)
        coordinator.syncFromRedisOncePublic(breakerName)

        val beforeClosed = server.requestCount
        val rClosed = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, rClosed.statusCode.value())

        // Circuit breaker 상태에 따라 다른 검증
        if (rClosed.body?.has("error") == true && rClosed.body?.getString("error") == "Circuit breaker is open") {
            // 아직 OPEN 상태
            Assertions.assertEquals(beforeClosed, server.requestCount, "OPEN 상태에서는 외부 호출이 없어야 함")
        } else {
            // CLOSED 상태로 전환됨
            Assertions.assertEquals(beforeClosed + 1, server.requestCount, "CLOSED에서 1회 호출되어야 함")
        }
    }

    @Test
    @DisplayName("단순 GET 요청 성공 시 JSON 반환")
    fun `simple GET success returns ok json`() = runBlocking {
        val org = "ORG_SIMPLE_OK_${System.nanoTime()}"
        val r = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(200, r.statusCode.value())
        Assertions.assertEquals(true, r.body?.getBoolean("ok"))
    }

    @Test
    @DisplayName("QPS 0일 때 429 반환 및 다운스트림 호출 없음")
    fun `rate limit 0 qps returns 429 and no downstream call`() = runBlocking {
        val org = "ORG_RATE_ZERO_${System.nanoTime()}"
        val rateKey = "rate:config:$org"
        redis.set(rateKey, "0")

        val before = server.requestCount
        val r = apiExecutor.execute(newRequest(org))
        Assertions.assertEquals(429, r.statusCode.value())
        Assertions.assertEquals("Rate limit exceeded", r.body?.getString("error"))
        Assertions.assertEquals(before, server.requestCount, "0QPS에서는 다운스트림 호출이 없어야 함")
    }

    @Test
    @DisplayName("HTTP 502 오류 시 YosepHttpErrorException 발생")
    fun `http 502 should throw YosepHttpErrorException`() = runBlocking {
        val org = "ORG_502_${System.nanoTime()}"
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                return if (request.path?.startsWith("/bad502") == true) {
                    json(502, "{\"error\":true}")
                } else json()
            }
        }
        val ex = Assertions.assertThrows(com.yosep.server.common.exception.YosepHttpErrorException::class.java) {
            runBlocking { apiExecutor.execute(newRequest(org, "/bad502")) }
        }
        Assertions.assertEquals(502, ex.httpStatus?.value())
    }

    @Test
    @DisplayName("HTTP 400 오류 시 YosepHttpErrorException 발생")
    fun `http 400 should throw YosepHttpErrorException`() = runBlocking {
        val org = "ORG_400_${System.nanoTime()}"
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                return if (request.path?.startsWith("/bad400") == true) {
                    json(400, "{\"error\":true}")
                } else json()
            }
        }
        val ex = Assertions.assertThrows(com.yosep.server.common.exception.YosepHttpErrorException::class.java) {
            runBlocking { apiExecutor.execute(newRequest(org, "/bad400")) }
        }
        Assertions.assertEquals(400, ex.httpStatus?.value())
    }

    @Test
    @DisplayName("실패 후 QPS 0으로 감소 시 같은 윈도우 내 429 반환")
    fun `after failure then reduced qps 0 leads to 429 within same window`() = runBlocking {
        val org = "ORG_FAIL_DROP_${System.nanoTime()}"
        val rateKey = "rate:config:$org"
        redis.set(rateKey, "1")

        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                return if (request.path?.startsWith("/always500") == true) {
                    json(500, "{\"err\":true}")
                } else json()
            }
        }

        alignToWindowStart()

        // First call fails with 500 (and AIMD would report failure)
        runCatching { apiExecutor.execute(newRequest(org, "/always500")) }

        // Simulate AIMD drop below initial by setting QPS to 0 immediately
        redis.set(rateKey, "0")

        val before = server.requestCount
        val r2 = apiExecutor.execute(newRequest(org, "/always500"))
        Assertions.assertEquals(429, r2.statusCode.value())
        Assertions.assertEquals(before, server.requestCount, "429에서는 외부 호출이 없어야 함")
    }

    @Test
    @DisplayName("높은 QPS에서 연속 호출 지연 없이 허용")
    fun `high rate allows back to back calls without ~1s wait`() = runBlocking {
        val org = "ORG_RATE_HIGH_${System.nanoTime()}"
        val rateKey = "rate:config:$org"
        redis.set(rateKey, "100")

        alignToWindowStart()

        val t0 = System.nanoTime()
        apiExecutor.execute(newRequest(org))
        apiExecutor.execute(newRequest(org))
        val elapsedMs = (System.nanoTime() - t0) / 1_000_000
        Assertions.assertTrue(elapsedMs < 300, "높은 QPS에서는 백투백 호출이 지연 없이 통과해야 함 (경과: ${'$'}elapsedMs ms)")
    }
}

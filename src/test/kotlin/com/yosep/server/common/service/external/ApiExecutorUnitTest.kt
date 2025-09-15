package com.yosep.server.common.service.external

import com.yosep.server.common.component.external.OrgWebClientManager
import com.yosep.server.common.component.ratelimit.ReactiveAimdSlowStartRateCoordinator
import com.yosep.server.common.component.ratelimit.ReactiveSlidingWindowRateLimiter
import com.yosep.server.common.service.circuitbreaker.ReactiveCircuitBreakerService
import com.yosep.server.domain.external.ApiRequest
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.http.HttpMethod
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

@OptIn(ExperimentalCoroutinesApi::class)
class ApiExecutorUnitTest {

    private lateinit var orgWebClientManager: OrgWebClientManager
    private lateinit var aimd: ReactiveAimdSlowStartRateCoordinator
    private lateinit var sliding: ReactiveSlidingWindowRateLimiter
    private lateinit var cbService: ReactiveCircuitBreakerService
    private lateinit var circuitBreaker: CircuitBreaker
    private lateinit var webClient: WebClient

    private lateinit var sut: ApiExecutor

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this, relaxed = true)
        orgWebClientManager = mockk()
        aimd = mockk()
        sliding = mockk()
        cbService = mockk()
        circuitBreaker = mockk()
        webClient = mockk(relaxed = true)

        sut = ApiExecutor(orgWebClientManager, aimd, sliding, cbService)
    }

    private fun newRequest(org: String): ApiRequest<String> = ApiRequest(
        body = null,
        headers = LinkedMultiValueMap(),
        code = "X",
        orgCode = org,
        domain = "",
        resource = "/ping",
        method = HttpMethod.GET,
        proxyUrl = URI.create("https://example.test/ping"),
        userId = 1L,
    )

    @Test
    fun `rate limit not acquired returns 429`() = runTest {
        // given
        val org = "ORG_RL_DENY"
        coEvery { aimd.getCurrentLimit(org) } returns 100
        coEvery { sliding.tryAcquireSuspend(org, 100, any()) } returns false

        // when
        val rsp = sut.execute(newRequest(org))

        // then
        assertEquals(429, rsp.statusCode.value())
        val bodyStr = rsp.body?.toString() ?: ""
        kotlin.test.assertTrue(bodyStr.contains("Rate limit exceeded"))
        coVerify(exactly = 0) { cbService.getCircuitBreaker(any()) }
    }

    @Test
    fun `circuit breaker permission denied returns fallback OK`() = runTest {
        // given
        val org = "ORG_CB_DENY"
        coEvery { aimd.getCurrentLimit(org) } returns 100
        coEvery { sliding.tryAcquireSuspend(org, 100, any()) } returns true
        every { cbService.getCircuitBreaker("${org}-mydata") } returns circuitBreaker
        every { circuitBreaker.tryAcquirePermission() } returns false

        // when
        val rsp: ResponseEntity<org.springframework.boot.configurationprocessor.json.JSONObject?> = sut.execute(newRequest(org))

        // then
        assertEquals(200, rsp.statusCode.value())
        assertEquals("Circuit breaker is open", rsp.body?.getString("error"))
        coVerify(exactly = 0) { aimd.reportSuccess(any()) }
        coVerify(exactly = 0) { aimd.reportFailure(any()) }
    }
}

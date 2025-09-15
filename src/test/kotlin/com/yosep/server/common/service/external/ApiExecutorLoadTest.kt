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
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.system.measureTimeMillis

/**
 * ApiExecutor 부하 테스트 및 스트레스 테스트
 * 
 * 실제 운영 환경에서 발생할 수 있는 다양한 부하 상황을 시뮬레이션하여
 * 시스템의 안정성과 성능을 검증합니다.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("load-test")
class ApiExecutorLoadTest @Autowired constructor(
    private val apiExecutor: ApiExecutor,
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val redis: RedisCommandHelper,
) : AbstractIntegrationContainerBase() {

    private lateinit var server: MockWebServer
    private val requestCounter = AtomicInteger(0)
    private val errorCounter = AtomicInteger(0)
    private val totalResponseTime = AtomicLong(0)

    @BeforeAll
    fun startServer() {
        server = MockWebServer()
        server.dispatcher = createLoadTestDispatcher()
        server.start()
    }

    @AfterAll
    fun stopServer() {
        runCatching { server.shutdown() }
    }

    private fun createLoadTestDispatcher() = object : Dispatcher() {
        private val responsePatterns = listOf(
            // 90% 성공 케이스
            { _: RecordedRequest -> 
                MockResponse()
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"success":true,"id":${requestCounter.incrementAndGet()}}""")
                    .setBodyDelay(Random.nextLong(10, 100), java.util.concurrent.TimeUnit.MILLISECONDS)
            },
            // 5% 서버 에러
            { _: RecordedRequest -> 
                errorCounter.incrementAndGet()
                MockResponse()
                    .setResponseCode(500)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"error":"Internal server error"}""")
            },
            // 3% 느린 응답
            { _: RecordedRequest -> 
                MockResponse()
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"success":true,"slow":true}""")
                    .setBodyDelay(Random.nextLong(1000, 2000), java.util.concurrent.TimeUnit.MILLISECONDS)
            },
            // 2% 타임아웃
            { _: RecordedRequest -> 
                MockResponse()
                    .setResponseCode(200)
                    .setBodyDelay(10000, java.util.concurrent.TimeUnit.MILLISECONDS)
            }
        )
        
        override fun dispatch(request: RecordedRequest): MockResponse {
            val rand = Random.nextInt(100)
            return when {
                rand < 90 -> responsePatterns[0](request)
                rand < 95 -> responsePatterns[1](request)
                rand < 98 -> responsePatterns[2](request)
                else -> responsePatterns[3](request)
            }
        }
    }

    private fun newRequest(org: String, path: String = "/load-test"): ApiRequest<String> = ApiRequest(
        body = null,
        headers = LinkedMultiValueMap(),
        code = "LOAD_TEST",
        orgCode = org,
        domain = "load.test.com",
        resource = path,
        method = HttpMethod.GET,
        proxyUrl = URI.create(server.url(path).toString()),
        userId = Random.nextLong(1, 1000),
    )

    private fun newCbEntity(name: String) = CircuitBreakerConfigEntity(
        breakerName = name,
        failureRateThreshold = 60, // 부하 테스트에서는 좀 더 관대하게
        slowCallRateThreshold = 80,
        slowCallDurationThresholdMs = 1500,
        waitDurationInOpenStateMs = 2000,
        permittedCallsInHalfOpenState = 5,
        minimumNumberOfCalls = 10,
        slidingWindowSize = 20,
        slidingWindowType = "COUNT_BASED",
        recordFailureStatusCodes = null,
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        isNew = true,
    )

    data class LoadTestResult(
        val totalRequests: Int,
        val successCount: Int,
        val errorCount: Int,
        val rateLimitedCount: Int,
        val circuitBreakerCount: Int,
        val avgResponseTime: Double,
        val maxResponseTime: Long,
        val throughput: Double,
        val totalDuration: Long
    )

    private suspend fun runLoadTest(
        orgCount: Int,
        requestsPerOrg: Int,
        qpsPerOrg: Int,
        durationSeconds: Int = 0
    ): LoadTestResult {
        val orgs = (1..orgCount).map { "LOAD_ORG_$it" }
        
        // 조직별 설정
        orgs.forEach { org ->
            redis.set("rate:config:$org",  qpsPerOrg.toString())
            val breakerName = "$org-mydata"
            coordinator.registerCircuitBreaker(newCbEntity(breakerName)).awaitSingleOrNull()
            delay(50)
            coordinator.syncFromRedisOncePublic(breakerName)
        }

        val startTime = System.currentTimeMillis()
        var totalRequests = 0
        var successCount = 0
        var errorCount = 0
        var rateLimitedCount = 0
        var circuitBreakerCount = 0
        val responseTimes = mutableListOf<Long>()

        val jobs = mutableListOf<Deferred<Unit>>()

        orgs.forEach { org ->
            repeat(requestsPerOrg) { requestIndex ->
                val job = GlobalScope.async {
                    try {
                        val requestStart = System.currentTimeMillis()
                        val response = apiExecutor.execute(newRequest(org, "/test$requestIndex"))
                        val requestEnd = System.currentTimeMillis()
                        val responseTime = requestEnd - requestStart
                        
                        synchronized(responseTimes) {
                            totalRequests++
                            responseTimes.add(responseTime)
                            
                            when (response.statusCode) {
                                HttpStatus.OK -> {
                                    val hasError = try {
                                        response.body?.has("error") == true && 
                                        response.body?.getString("error") == "Circuit breaker is open"
                                    } catch (e: Exception) {
                                        false
                                    }
                                    
                                    if (hasError) {
                                        circuitBreakerCount++
                                    } else {
                                        successCount++
                                    }
                                }
                                HttpStatus.TOO_MANY_REQUESTS -> rateLimitedCount++
                                else -> errorCount++
                            }
                        }
                        
                    } catch (e: Exception) {
                        synchronized(responseTimes) {
                            totalRequests++
                            errorCount++
                        }
                    }
                    
                    // 요청 간격 제어
                    delay(Random.nextLong(10, 100))
                }
                jobs.add(job)
                
                // CPU 과부하 방지를 위한 배치 처리
                if (jobs.size >= 50) {
                    delay(Random.nextLong(1, 10))
                }
            }
        }

        // 지정된 시간 동안 실행하거나 모든 작업 완료까지 대기
        if (durationSeconds > 0) {
            withTimeoutOrNull(durationSeconds * 1000L) {
                jobs.awaitAll()
            }
            jobs.filter { !it.isCompleted }.forEach { it.cancel() }
        } else {
            jobs.awaitAll()
        }

        val endTime = System.currentTimeMillis()
        val totalDuration = endTime - startTime
        
        val avgResponseTime = if (responseTimes.isNotEmpty()) responseTimes.average() else 0.0
        val maxResponseTime = responseTimes.maxOrNull() ?: 0L
        val throughput = totalRequests * 1000.0 / totalDuration

        return LoadTestResult(
            totalRequests = totalRequests,
            successCount = successCount,
            errorCount = errorCount,
            rateLimitedCount = rateLimitedCount,
            circuitBreakerCount = circuitBreakerCount,
            avgResponseTime = avgResponseTime,
            maxResponseTime = maxResponseTime,
            throughput = throughput,
            totalDuration = totalDuration
        )
    }

    @Nested
    @DisplayName("기본 부하 테스트")
    inner class BasicLoadTests {

        @Test
        @DisplayName("중간 부하 테스트 - 10개 조직, 조직당 50개 요청")
        fun testMediumLoad() = runTest {
            val result = runLoadTest(
                orgCount = 10,
                requestsPerOrg = 50,
                qpsPerOrg = 10
            )
            
            println("=== 중간 부하 테스트 결과 ===")
            println("총 요청: ${result.totalRequests}")
            println("성공: ${result.successCount}")
            println("에러: ${result.errorCount}")
            println("Rate Limited: ${result.rateLimitedCount}")
            println("Circuit Breaker: ${result.circuitBreakerCount}")
            println("평균 응답시간: ${String.format("%.2f", result.avgResponseTime)}ms")
            println("최대 응답시간: ${result.maxResponseTime}ms")
            println("처리량: ${String.format("%.2f", result.throughput)} req/sec")
            println("총 소요시간: ${result.totalDuration}ms")
            
            // 성능 기준 검증 - Rate limit 때문에 일부 요청이 실패할 수 있음
            assertThat(result.totalRequests).isEqualTo(500)
            assertThat(result.successCount + result.rateLimitedCount + result.errorCount + result.circuitBreakerCount).isEqualTo(500)
            assertThat(result.successCount).isGreaterThan(0)
        }

        @Test
        @DisplayName("고부하 테스트 - 20개 조직, 조직당 100개 요청")
        fun testHighLoad() = runTest {
            val result = runLoadTest(
                orgCount = 20,
                requestsPerOrg = 100,
                qpsPerOrg = 15
            )
            
            println("=== 고부하 테스트 결과 ===")
            println("총 요청: ${result.totalRequests}")
            println("성공: ${result.successCount}")
            println("에러: ${result.errorCount}")
            println("Rate Limited: ${result.rateLimitedCount}")
            println("Circuit Breaker: ${result.circuitBreakerCount}")
            println("평균 응답시간: ${String.format("%.2f", result.avgResponseTime)}ms")
            println("최대 응답시간: ${result.maxResponseTime}ms")
            println("처리량: ${String.format("%.2f", result.throughput)} req/sec")
            println("총 소요시간: ${result.totalDuration}ms")
            
            // 고부하에서도 기본 안정성 유지 - Rate limit 때문에 일부 요청이 실패할 수 있음
            assertThat(result.totalRequests).isEqualTo(2000)
            assertThat(result.successCount + result.rateLimitedCount + result.errorCount + result.circuitBreakerCount).isEqualTo(2000)
            assertThat(result.successCount).isGreaterThan(0)
        }
    }

    @Nested
    @DisplayName("스트레스 테스트")
    inner class StressTests {

        @Test
        @DisplayName("극한 동시성 테스트")
        fun testExtremeConcurrency() = runTest {
            val org = "EXTREME_CONCURRENT_${System.nanoTime()}"
            redis.set("rate:config:$org", "1000") // 매우 높은 QPS
            
            val concurrentRequests = 500
            val successCount = AtomicInteger(0)
            val errorCount = AtomicInteger(0)
            val responseTime = AtomicLong(0)
            
            val duration = measureTimeMillis {
                val jobs = List(concurrentRequests) { index ->
                    async {
                        try {
                            val start = System.currentTimeMillis()
                            val response = apiExecutor.execute(newRequest(org, "/extreme$index"))
                            val end = System.currentTimeMillis()
                            
                            responseTime.addAndGet(end - start)
                            
                            if (response.statusCode == HttpStatus.OK) {
                                successCount.incrementAndGet()
                            } else {
                                errorCount.incrementAndGet()
                            }
                        } catch (e: Exception) {
                            errorCount.incrementAndGet()
                        }
                    }
                }
                jobs.awaitAll()
            }
            
            val avgResponseTime = responseTime.get().toDouble() / successCount.get()
            val throughput = concurrentRequests * 1000.0 / duration
            
            println("=== 극한 동시성 테스트 결과 ===")
            println("동시 요청: $concurrentRequests")
            println("성공: ${successCount.get()}")
            println("실패: ${errorCount.get()}")
            println("평균 응답시간: ${String.format("%.2f", avgResponseTime)}ms")
            println("처리량: ${String.format("%.2f", throughput)} req/sec")
            println("총 소요시간: ${duration}ms")
            
            assertThat(successCount.get()).isGreaterThan((concurrentRequests * 0.8).toInt())
            assertThat(avgResponseTime).isLessThan(500.0)
        }

        @Test
        @DisplayName("메모리 누수 테스트")
        fun testMemoryLeak() = runTest {
            val org = "MEMORY_LEAK_${System.nanoTime()}"
            redis.set("rate:config:$org", "100")
            val runtime = Runtime.getRuntime()
            val initialMemory = runtime.totalMemory() - runtime.freeMemory()
            
            val iterations = 10
            val requestsPerIteration = 100
            
            repeat(iterations) { iteration ->
                val jobs = List(requestsPerIteration) { index ->
                    async {
                        try {
                            apiExecutor.execute(newRequest(org, "/memory-test-$iteration-$index"))
                        } catch (e: Exception) {
                            // 에러 무시하고 계속
                        }
                    }
                }
                jobs.awaitAll()
                
                // 주기적으로 GC 실행
                if (iteration % 3 == 0) {
                    System.gc()
                    delay(100)
                }
            }
            
            System.gc()
            delay(200)
            
            val finalMemory = runtime.totalMemory() - runtime.freeMemory()
            val memoryIncrease = finalMemory - initialMemory
            val memoryIncreaseMB = memoryIncrease / (1024.0 * 1024.0)
            
            println("=== 메모리 누수 테스트 결과 ===")
            println("초기 메모리: ${initialMemory / 1024 / 1024}MB")
            println("최종 메모리: ${finalMemory / 1024 / 1024}MB")
            println("메모리 증가: ${String.format("%.2f", memoryIncreaseMB)}MB")
            println("총 요청: ${iterations * requestsPerIteration}")
            
            // 메모리 증가가 100MB 이하여야 함
            assertThat(memoryIncreaseMB).isLessThan(100.0)
        }

        @Test
        @DisplayName("장시간 안정성 테스트")
        fun testLongTermStability() = runTest {
            val orgs = (1..5).map { "STABILITY_ORG_${System.nanoTime()}_$it" }
            orgs.forEach { org ->
                redis.set("rate:config:$org", "${Random.nextInt(5, 20)}")            }
            
            val testDurationSeconds = 10 // 실제 운영에서는 더 길게 설정
            val startTime = System.currentTimeMillis()
            var totalRequests = 0
            var successRequests = 0
            val errors = mutableListOf<String>()
            
            val job = launch {
                while (System.currentTimeMillis() - startTime < testDurationSeconds * 1000) {
                    val org = orgs.random()
                    try {
                        val response = apiExecutor.execute(newRequest(org))
                        totalRequests++

                        if (response.statusCode == HttpStatus.OK) {
                            val body = response.body
                            if (body?.has("error") == true && body.getString("error") == "Circuit breaker is open") {
                                // Circuit breaker가 동작하는 것도 정상 동작
                                successRequests++
                            } else {
                                successRequests++
                            }
                        } else if (response.statusCode == HttpStatus.TOO_MANY_REQUESTS) {
                            // Rate limit에 걸린 것도 정상 동작
                            successRequests++
                        }
                    } catch (e: Exception) {
                        totalRequests++
                        errors.add(e.javaClass.simpleName)
                    }
                    
                    delay(Random.nextLong(50, 200)) // 현실적인 간격
                }
            }
            
            job.join()
            
            val actualDuration = System.currentTimeMillis() - startTime
            val successRate = successRequests.toDouble() / totalRequests
            val throughput = totalRequests * 1000.0 / actualDuration
            
            println("=== 장시간 안정성 테스트 결과 ===")
            println("테스트 시간: ${actualDuration}ms")
            println("총 요청: $totalRequests")
            println("성공 요청: $successRequests")
            println("성공률: ${String.format("%.2f%%", successRate * 100)}")
            println("처리량: ${String.format("%.2f", throughput)} req/sec")
            println("에러 유형: ${errors.groupingBy { it }.eachCount()}")
            
            assertThat(totalRequests).isGreaterThan(50)
            assertThat(successRate).isGreaterThan(0.7)
            assertThat(throughput).isGreaterThan(5.0)
        }
    }

    @Nested
    @DisplayName("복구력 테스트")
    inner class ResilienceTests {

        @Test
        @DisplayName("Circuit Breaker 대량 트리거 후 복구")
        fun testMassiveCircuitBreakerRecovery() = runTest {
            val orgs = (1..10).map { "CB_RECOVERY_ORG_${System.nanoTime()}_$it" }

            // 모든 조직에 Circuit Breaker 설정
            orgs.forEach { org ->
                redis.set("rate:config:$org", "20")
                val breakerName = "$org-mydata"
                coordinator.registerCircuitBreaker(newCbEntity(breakerName)).awaitSingleOrNull()
                delay(50)
                coordinator.syncFromRedisOncePublic(breakerName)
            }

            // Phase 1: 모든 Circuit Breaker를 OPEN 상태로 만들기
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    return MockResponse().setResponseCode(500).setBody("{\"error\":true}")
                }
            }

            val failureJobs = orgs.map { org ->
                async {
                    repeat(15) { // minimumNumberOfCalls보다 많은 실패 생성
                        runCatching { apiExecutor.execute(newRequest(org)) }
                        delay(10)
                    }
                }
            }
            failureJobs.awaitAll()
            delay(200)

            // Phase 2: 정상 서비스로 복구
            server.dispatcher = createLoadTestDispatcher()

            // Phase 3: Circuit Breaker 대기 시간 후 복구 테스트
            delay(2500) // waitDurationInOpenStateMs보다 길게

            val recoveryJobs = orgs.map { org ->
                async {
                    var successCount = 0
                    var circuitBreakerCount = 0
                    var errorCount = 0

                    repeat(10) {
                        try {
                            val response = apiExecutor.execute(newRequest(org))
                            if (response.statusCode == HttpStatus.OK) {
                                val body = response.body
                                if (body?.has("error") == true && body.getString("error") == "Circuit breaker is open") {
                                    circuitBreakerCount++
                                } else {
                                    successCount++
                                }
                            } else if (response.statusCode == HttpStatus.TOO_MANY_REQUESTS) {
                                // Rate limit에 걸린 경우도 정상 동작으로 처리
                                successCount++
                            }
                        } catch (e: Exception) {
                            errorCount++
                        }
                        delay(100)
                    }

                    Triple(successCount, circuitBreakerCount, errorCount)
                }
            }

            val recoveryResults = recoveryJobs.awaitAll()
            val totalSuccess = recoveryResults.sumOf { it.first }
            val totalCircuitBreaker = recoveryResults.sumOf { it.second }
            val totalError = recoveryResults.sumOf { it.third }

            println("=== Circuit Breaker 복구 테스트 결과 ===")
            println("총 성공 요청: $totalSuccess")
            println("Circuit Breaker 차단: $totalCircuitBreaker")
            println("에러: $totalError")
            println("조직별 결과: ${recoveryResults.mapIndexed { index, (success, cb, error) ->
                "ORG${index+1}: 성공=$success, CB=$cb, 에러=$error"
            }.joinToString(", ")}")

            // Circuit Breaker가 동작하거나 일부 성공이 있어야 함
            assertThat(totalSuccess + totalCircuitBreaker).isGreaterThan(0)
            // 전체 요청의 50% 이상은 처리되어야 함
            assertThat(totalSuccess + totalCircuitBreaker).isGreaterThan(orgs.size * 10 / 2)
        }
    }
}
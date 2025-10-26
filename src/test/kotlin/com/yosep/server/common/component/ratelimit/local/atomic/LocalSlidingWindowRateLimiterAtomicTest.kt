package com.yosep.server.common.component.ratelimit.local.atomic

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.system.measureTimeMillis

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class LocalSlidingWindowRateLimiterAtomicTest {

    private lateinit var rateLimiter: LocalSlidingWindowCounterRateLimiterAtomic

    @BeforeEach
    fun setUp() {
        rateLimiter = LocalSlidingWindowCounterRateLimiterAtomic()
    }

    @Test
    fun `기본 rate limiting 테스트`() = runTest {
        val key = "test-key"
        val maxCount = 10
        val windowMs = 1000L

        // 10개 요청은 성공
        repeat(maxCount) { i ->
            val result = rateLimiter.tryAcquire(key, maxCount, windowMs)
            assertThat(result).describedAs("Request $i should succeed").isTrue()
        }

        // 11번째 요청은 실패
        val result = rateLimiter.tryAcquire(key, maxCount, windowMs)
        assertThat(result).describedAs("11th request should fail").isFalse()
    }

    @Test
    fun `슬라이딩 윈도우 동작 테스트`() = runBlocking {
        val key = "sliding-key"
        val maxCount = 5
        val windowMs = 1000L

        // 처음 5개 요청
        repeat(maxCount) {
            assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isTrue()
        }

        // 6번째는 실패
        assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isFalse()

        // 500ms 대기
        delay(500)

        // 여전히 실패 (윈도우 내)
        assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isFalse()

        // 600ms 더 대기 (총 1100ms)
        delay(600)

        // 이제 성공 (첫 요청이 윈도우 밖으로)
        assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isTrue()
    }

    @Test
    fun `버킷 기반 카운팅 정확도 테스트`() = runBlocking {
        val key = "bucket-key"
        val maxCount = 10
        val windowMs = 1000L

        // 100ms 간격으로 요청 (각 버킷에 1개씩)
        repeat(5) { i ->
            assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs))
                .describedAs("Request $i should succeed")
                .isTrue()
            delay(100)
        }

        // 현재 사용량 확인
        val usage = rateLimiter.getCurrentUsage(key, windowMs)
        assertThat(usage).isEqualTo(5)

        // 500ms 더 대기
        delay(500)

        // 사용량 감소 확인 (오래된 버킷이 윈도우 밖으로)
        val newUsage = rateLimiter.getCurrentUsage(key, windowMs)
        assertThat(newUsage).isLessThan(5)
    }

    @Test
    fun `정확한(precise) 카운팅 테스트`() = runBlocking {
        val key = "precise-key"
        val maxCount = 100
        val windowMs = 1000L

        // 빠르게 50개 요청
        repeat(50) {
            rateLimiter.tryAcquire(key, maxCount, windowMs, usePrecise = true)
        }

        // 정확한 사용량과 일반 사용량 비교
        val normalUsage = rateLimiter.getCurrentUsage(key, windowMs)
        val preciseUsage = rateLimiter.getPreciseUsage(key, windowMs)

        println("Normal usage: $normalUsage, Precise usage: $preciseUsage")

        // 둘 다 50 근처여야 함
        assertThat(normalUsage).isGreaterThanOrEqualTo(45)
        assertThat(preciseUsage).isGreaterThanOrEqualTo(45.0)
    }

    @Test
    fun `다중 키 독립성 테스트`() = runTest {
        val key1 = "key1"
        val key2 = "key2"
        val maxCount = 5
        val windowMs = 1000L

        // key1에 5개 요청
        repeat(maxCount) {
            assertThat(rateLimiter.tryAcquire(key1, maxCount, windowMs)).isTrue()
        }

        // key1은 실패
        assertThat(rateLimiter.tryAcquire(key1, maxCount, windowMs)).isFalse()

        // key2는 성공
        repeat(maxCount) {
            assertThat(rateLimiter.tryAcquire(key2, maxCount, windowMs)).isTrue()
        }

        // key2도 실패
        assertThat(rateLimiter.tryAcquire(key2, maxCount, windowMs)).isFalse()
    }

    @Test
    fun `동시성 테스트`() = runBlocking {
        val key = "concurrent-key"
        val maxCount = 100
        val windowMs = 1000L
        val numThreads = 10
        val requestsPerThread = 15

        val successCount = java.util.concurrent.atomic.AtomicInteger(0)
        val failCount = java.util.concurrent.atomic.AtomicInteger(0)

        // 여러 코루틴에서 동시 요청
        val jobs = List(numThreads) {
            launch(Dispatchers.Default) {
                repeat(requestsPerThread) {
                    val result = rateLimiter.tryAcquire(key, maxCount, windowMs)
                    if (result) {
                        successCount.incrementAndGet()
                    } else {
                        failCount.incrementAndGet()
                    }
                    delay(5) // 약간의 지연
                }
            }
        }

        jobs.forEach { it.join() }

        println("Success: ${successCount.get()}, Fail: ${failCount.get()}")

        // 성공한 요청이 maxCount를 초과하지 않아야 함
        assertThat(successCount.get()).isLessThanOrEqualTo(maxCount)
        // 전체 요청 수 확인
        assertThat(successCount.get() + failCount.get()).isEqualTo(numThreads * requestsPerThread)
    }

    @Test
    fun `리셋 테스트`() = runTest {
        val key = "reset-key"
        val maxCount = 5
        val windowMs = 1000L

        // 5개 요청 소진
        repeat(maxCount) {
            assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isTrue()
        }

        // 6번째는 실패
        assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isFalse()

        // 리셋
        rateLimiter.reset(key)

        // 리셋 후 다시 성공
        repeat(maxCount) {
            assertThat(rateLimiter.tryAcquire(key, maxCount, windowMs)).isTrue()
        }
    }

    @Test
    fun `통계 정보 테스트`() = runTest {
        // 여러 키에 요청
        val keys = listOf("key1", "key2", "key3")
        keys.forEach { key ->
            repeat(3) {
                rateLimiter.tryAcquire(key, 10, 1000L)
            }
        }

        // 통계 조회
        val stats = rateLimiter.getStats()

        assertThat(stats["totalKeys"]).isEqualTo(3)
        assertThat(stats["activeWindows"]).isEqualTo(3)
        assertThat(stats["totalRequests"] as Long).isGreaterThanOrEqualTo(9)
        assertThat(stats["bucketSizeMs"]).isEqualTo(100L)
        assertThat(stats["numBuckets"]).isEqualTo(600) // 60초 / 100ms
        assertThat(stats["maxWindowMs"]).isEqualTo(60000L) // 60초
    }

    @Test
    fun `상세 통계 테스트`() = runTest {
        val key = "detail-key"
        val maxCount = 10
        val windowMs = 1000L

        // 5개 요청
        repeat(5) {
            rateLimiter.tryAcquire(key, maxCount, windowMs)
        }

        // 상세 통계 조회
        val details = rateLimiter.getDetailedStats(key, windowMs)

        assertThat(details).isNotNull
        assertThat(details!!["key"]).isEqualTo(key)
        assertThat(details["currentCount"] as Long).isEqualTo(5)
        assertThat(details["windowMs"]).isEqualTo(windowMs)
        assertThat(details["lastUpdateTime"]).isNotNull()
    }

    @Test
    fun `버스트 트래픽 처리 테스트`() = runBlocking {
        val key = "burst-key"
        val maxCount = 50
        val windowMs = 1000L

        // 버스트 트래픽 시뮬레이션
        val burstSize = 30
        var successCount = 0

        // 첫 버스트
        repeat(burstSize) {
            if (rateLimiter.tryAcquire(key, maxCount, windowMs)) {
                successCount++
            }
        }

        println("First burst success: $successCount")
        assertThat(successCount).isEqualTo(burstSize)

        // 200ms 대기 후 두 번째 버스트
        delay(200)
        var secondBurstSuccess = 0
        repeat(burstSize) {
            if (rateLimiter.tryAcquire(key, maxCount, windowMs)) {
                secondBurstSuccess++
            }
        }

        println("Second burst success: $secondBurstSuccess")
        // 두 번째 버스트는 부분적으로만 성공
        assertThat(secondBurstSuccess).isLessThanOrEqualTo(maxCount - burstSize)
    }

    @Test
    fun `긴 윈도우 테스트 (30초)`() = runBlocking {
        val key = "long-window-key"
        val maxCount = 100
        val windowMs = 30000L // 30초 윈도우 (최대 60초까지 가능)

        // 10초 동안 분산된 요청
        val startTime = System.currentTimeMillis()
        var successCount = 0

        while (System.currentTimeMillis() - startTime < 10000) {
            if (rateLimiter.tryAcquire(key, maxCount, windowMs)) {
                successCount++
            }
            delay(100) // 100ms 간격
        }

        println("Success count in 10 seconds: $successCount")
        assertThat(successCount).isLessThanOrEqualTo(maxCount)

        // 현재 사용량 확인
        val usage = rateLimiter.getCurrentUsage(key, windowMs)
        println("Current usage: $usage")
        assertThat(usage).isEqualTo(successCount.toLong())
    }

    @Test
    fun `최대 윈도우 테스트 (60초)`() = runBlocking {
        val key = "max-window-key"
        val maxCount = 200
        val windowMs = 60000L // 60초 (1분) - 최대 윈도우

        // 초기 50개 요청
        var initialSuccess = 0
        repeat(50) {
            if (rateLimiter.tryAcquire(key, maxCount, windowMs)) {
                initialSuccess++
            }
        }

        println("Initial requests: $initialSuccess")
        assertThat(initialSuccess).isEqualTo(50)

        // 20초 후 추가 요청
        delay(20000)

        var additionalSuccess = 0
        repeat(50) {
            if (rateLimiter.tryAcquire(key, maxCount, windowMs)) {
                additionalSuccess++
            }
        }

        println("Additional requests after 20s: $additionalSuccess")
        assertThat(additionalSuccess).isEqualTo(50)

        // 전체 사용량 확인 (60초 윈도우 내 모든 요청)
        val totalUsage = rateLimiter.getCurrentUsage(key, windowMs)
        println("Total usage in 60s window: $totalUsage")
        assertThat(totalUsage).isEqualTo(100L)

        // 정밀 사용량도 확인
        val preciseUsage = rateLimiter.getPreciseUsage(key, windowMs)
        println("Precise usage: $preciseUsage")
        assertThat(preciseUsage).isGreaterThanOrEqualTo(95.0) // 약간의 오차 허용
    }

    @Test
    fun `clearAll 테스트`() = runTest {
        // 여러 키에 데이터 추가
        val keys = (1..5).map { "key$it" }
        keys.forEach { key ->
            repeat(3) {
                rateLimiter.tryAcquire(key, 10, 1000L)
            }
        }

        // 통계 확인
        var stats = rateLimiter.getStats()
        assertThat(stats["totalKeys"]).isEqualTo(5)

        // 전체 클리어
        rateLimiter.clearAll()

        // 클리어 후 통계
        stats = rateLimiter.getStats()
        assertThat(stats["totalKeys"]).isEqualTo(0)
        assertThat(stats["totalRequests"]).isEqualTo(0L)
    }

    @Test
    fun `성능 테스트 - 높은 처리량`() = runBlocking {
        val key = "perf-key"
        val maxCount = 10000
        val windowMs = 1000L
        val totalRequests = 100000

        val elapsed = measureTimeMillis {
            repeat(totalRequests) {
                rateLimiter.tryAcquire(key, maxCount, windowMs)
            }
        }

        val throughput = totalRequests * 1000.0 / elapsed
        println("Throughput: ${String.format("%.0f", throughput)} ops/sec")
        println("Time per operation: ${elapsed * 1000.0 / totalRequests} μs")

        assertThat(throughput).isGreaterThan(100000.0) // 최소 10만 ops/sec
    }
}
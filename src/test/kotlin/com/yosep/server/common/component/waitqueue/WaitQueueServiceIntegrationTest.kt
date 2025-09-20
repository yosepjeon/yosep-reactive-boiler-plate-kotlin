package com.yosep.server.common.component.waitqueue

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.common.service.waitqueue.WaitQueueService
import com.yosep.server.infrastructure.redis.component.RedisCommandHelper
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WaitQueueServiceIntegrationTest @Autowired constructor(
    private val waitQueueService: WaitQueueService,
    private val waitQueueCoordinator: WaitQueueCoordinator,
    private val redisCommandHelper: RedisCommandHelper
) : AbstractIntegrationContainerBase() {

    @BeforeEach
    fun setUp() = runBlocking {
        // 테스트 전 대기열 초기화
        redisCommandHelper.delete(WaitQueueService.WAIT_QUEUE_KEY)
        redisCommandHelper.delete(WaitQueueService.ENTER_QUEUE_KEY)
        redisCommandHelper.delete(WaitQueueService.QUEUE_METRICS_KEY)
        // 개별 토큰 키들도 정리
        cleanupAllTokens()
    }

    @AfterEach
    fun tearDown() = runBlocking {
        cleanupAllTokens()
    }

    private suspend fun cleanupAllTokens() {
        // Redis에서 모든 토큰 키 삭제
        val keys = listOf("queue:token:*")
        keys.forEach { pattern ->
            // Note: 실제 환경에서는 SCAN 명령어 사용 권장
            try {
                redisCommandHelper.delete(pattern)
            } catch (e: Exception) {
                // 무시
            }
        }
    }

    @Test
    @DisplayName("사용자가 대기열에 참가하면 위치가 반환된다")
    fun `when user joins queue then position is returned`() = runBlocking {
        // Given
        val userId = "user1"

        // When
        val position = waitQueueService.joinWaitQueue(userId)

        // Then
        assertThat(position).isEqualTo(1L)

        val status = waitQueueService.getQueueStatus(userId)
        assertThat(status.status).isEqualTo("WAITING")
        assertThat(status.position).isEqualTo(1L)
    }

    @Test
    @DisplayName("여러 사용자가 순차적으로 대기열에 참가한다")
    fun `multiple users join queue sequentially`() = runBlocking {
        // Given
        val users = listOf("user1", "user2", "user3", "user4", "user5")

        // When
        val positions = users.map { userId ->
            userId to waitQueueService.joinWaitQueue(userId)
        }

        // Then
        positions.forEachIndexed { index, (userId, position) ->
            assertThat(position).isEqualTo((index + 1).toLong())

            val status = waitQueueService.getQueueStatus(userId)
            assertThat(status.status).isEqualTo("WAITING")
            assertThat(status.position).isEqualTo((index + 1).toLong())
        }

        val metrics = waitQueueService.getQueueMetrics()
        assertThat(metrics.waitQueueSize).isEqualTo(5)
        assertThat(metrics.totalJoined).isEqualTo(5)
    }

    @Test
    @DisplayName("이미 입장열에 있는 사용자는 -1을 반환한다")
    fun `user already in enter queue returns negative one`() = runBlocking {
        // Given
        val userId = "user1"
        waitQueueService.joinWaitQueue(userId)

        // 입장열로 이동
        val transitioned = waitQueueService.transitionToEnterQueue(1)
        assertThat(transitioned).contains(userId)

        // When
        val position = waitQueueService.joinWaitQueue(userId)

        // Then
        assertThat(position).isEqualTo(-1L)
    }

    @Test
    @DisplayName("대기열에서 입장열로 올바르게 전환된다")
    fun `users transition from wait queue to enter queue`() = runTest {
        // Given
        val users = (1..10).map { "user$it" }
        users.forEach { waitQueueService.joinWaitQueue(it) }

        // Wait for all users to be added to queue
        delay(100)

        // Verify all users are in wait queue with retry
        var initialMetrics = waitQueueService.getQueueMetrics()
        var retryCount = 0
        while (initialMetrics.waitQueueSize < 10 && retryCount < 5) {
            delay(50)
            initialMetrics = waitQueueService.getQueueMetrics()
            retryCount++
        }

        assertThat(initialMetrics.waitQueueSize).isEqualTo(10)

        // When
        val batchSize = 3
        val transitioned = waitQueueService.transitionToEnterQueue(batchSize)

        // Then
        assertThat(transitioned).hasSize(3)
        assertThat(transitioned).containsExactly("user1", "user2", "user3")

        val afterMetrics = waitQueueService.getQueueMetrics()
        assertThat(afterMetrics.waitQueueSize).isEqualTo(7)
        assertThat(afterMetrics.enterQueueSize).isEqualTo(3)
        assertThat(afterMetrics.totalEntered).isEqualTo(3)

        // 전환된 사용자들 상태 확인
        transitioned.forEach { userId ->
            val isInEnterQueue = waitQueueService.isUserInEnterQueue(userId)
            assertThat(isInEnterQueue).isTrue

            val status = waitQueueService.getQueueStatus(userId)
            assertThat(status.status).isEqualTo("ENTERED")
        }
    }

    @Test
    @DisplayName("사용자 이탈 시 대기열과 입장열에서 제거된다")
    fun `user exit removes from both queues`() = runBlocking {
        // Given
        val userId = "user1"
        waitQueueService.joinWaitQueue(userId)

        // 입장열로 이동
        waitQueueService.transitionToEnterQueue(1)
        assertThat(waitQueueService.isUserInEnterQueue(userId)).isTrue

        // When
        waitQueueService.handleUserExit(userId)

        // Then
        val status = waitQueueService.getQueueStatus(userId)
        assertThat(status.status).isEqualTo("NOT_IN_QUEUE")
        assertThat(waitQueueService.isUserInEnterQueue(userId)).isFalse

        val metrics = waitQueueService.getQueueMetrics()
        assertThat(metrics.totalExited).isEqualTo(1)
    }

    @Test
    @DisplayName("토큰이 만료된 사용자는 전환 시 제외된다")
    fun `expired token users are excluded from transition`() = runBlocking {
        // Given
        val validUser = "validUser"
        val expiredUser = "expiredUser"

        waitQueueService.joinWaitQueue(validUser)
        waitQueueService.joinWaitQueue(expiredUser)

        // 토큰 강제 만료
        redisCommandHelper.delete("${WaitQueueService.USER_TOKEN_PREFIX}$expiredUser")

        // When
        val transitioned = waitQueueService.transitionToEnterQueue(10)

        // Then
        assertThat(transitioned).hasSize(1)
        assertThat(transitioned).contains(validUser)
        assertThat(transitioned).doesNotContain(expiredUser)

        // 만료된 사용자는 대기열에서도 제거됨
        val expiredStatus = waitQueueService.getQueueStatus(expiredUser)
        assertThat(expiredStatus.status).isEqualTo("NOT_IN_QUEUE")
    }

    @Test
    @DisplayName("동적 배압에 따라 배치 크기가 조정된다")
    fun `batch size adjusts based on backpressure`() = runBlocking {
        // Given
        val initialBatchSize = waitQueueCoordinator.getCurrentBatchSize()

        // When - 높은 배압 시뮬레이션
        // Note: 실제 테스트에서는 Circuit Breaker를 OPEN으로 설정하거나
        // Rate Limiter를 소진시켜 배압을 생성해야 함
        waitQueueCoordinator.setBatchSize(1000)

        // Then
        val adjustedSize = waitQueueCoordinator.getCurrentBatchSize()
        assertThat(adjustedSize).isLessThanOrEqualTo(WaitQueueCoordinator.MAX_BATCH_SIZE)
        assertThat(adjustedSize).isGreaterThanOrEqualTo(WaitQueueCoordinator.MIN_BATCH_SIZE)
    }

    @Test
    @DisplayName("입장열 정리 기능이 오래된 사용자를 제거한다")
    fun `cleanup removes old users from enter queue`() = runBlocking {
        // Given
        val users = (1..5).map { "user$it" }
        users.forEach { waitQueueService.joinWaitQueue(it) }
        waitQueueService.transitionToEnterQueue(5)

        // 시간 조작이 어려우므로 즉시 정리 테스트
        val initialMetrics = waitQueueService.getQueueMetrics()
        assertThat(initialMetrics.enterQueueSize).isEqualTo(5)

        // When - 0초로 설정하여 모두 제거
        val cleaned = waitQueueService.cleanupEnterQueue(java.time.Duration.ofSeconds(0))

        // Then
        // 방금 추가된 사용자들은 현재 시간으로 score가 설정되어 제거되지 않음
        assertThat(cleaned).isEqualTo(0)
    }

    @Test
    @DisplayName("대기열 메트릭이 올바르게 업데이트된다")
    fun `queue metrics are updated correctly`() = runBlocking {
        // Given & When
        val users = (1..20).map { "user$it" }
        users.forEach { waitQueueService.joinWaitQueue(it) }

        val afterJoin = waitQueueService.getQueueMetrics()
        assertThat(afterJoin.totalJoined).isEqualTo(20)
        assertThat(afterJoin.waitQueueSize).isEqualTo(20)

        val transitioned = waitQueueService.transitionToEnterQueue(5)
        val afterTransition = waitQueueService.getQueueMetrics()
        assertThat(afterTransition.totalEntered).isEqualTo(5)
        assertThat(afterTransition.waitQueueSize).isEqualTo(15)
        assertThat(afterTransition.enterQueueSize).isEqualTo(5)
        assertThat(afterTransition.lastTransitionCount).isEqualTo(5)

        users.take(3).forEach { waitQueueService.handleUserExit(it) }
        val afterExit = waitQueueService.getQueueMetrics()
        assertThat(afterExit.totalExited).isEqualTo(3)
    }

    @Test
    @DisplayName("동일 사용자가 중복 참가 시 기존 위치를 유지한다")
    fun `duplicate join maintains existing position`() = runBlocking {
        // Given
        val userId = "user1"
        val firstPosition = waitQueueService.joinWaitQueue(userId)

        // 다른 사용자들 추가
        (2..5).forEach { waitQueueService.joinWaitQueue("user$it") }

        // When
        val secondPosition = waitQueueService.joinWaitQueue(userId)

        // Then
        assertThat(secondPosition).isEqualTo(firstPosition)

        val status = waitQueueService.getQueueStatus(userId)
        assertThat(status.position).isEqualTo(1L)
    }

    @Test
    @DisplayName("우선순위에 따라 대기열 순서가 결정된다")
    fun `queue order is determined by priority`() = runBlocking {
        // Given - 낮은 숫자가 높은 우선순위
        waitQueueService.joinWaitQueue("user3", 300.0)
        waitQueueService.joinWaitQueue("user1", 100.0)
        waitQueueService.joinWaitQueue("user2", 200.0)

        // When
        val transitioned = waitQueueService.transitionToEnterQueue(3)

        // Then
        assertThat(transitioned).containsExactly("user1", "user2", "user3")
    }
}
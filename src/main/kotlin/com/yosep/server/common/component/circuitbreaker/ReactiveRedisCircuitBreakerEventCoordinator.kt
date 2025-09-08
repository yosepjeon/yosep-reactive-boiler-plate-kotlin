package com.yosep.server.common.component.circuitbreaker

import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.common.exception.YosepHttpErrorException
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.netty.handler.timeout.TimeoutException
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.redisson.api.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientRequestException
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern

@Component
class ReactiveRedisCircuitBreakerEventCoordinator(
    private val redisson: RedissonReactiveClient,
    private val cbRegistry: CircuitBreakerRegistry,
    private val om: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val KEY_STATE = "cb:{%s}:state"
        private const val KEY_LOCK = "cb:{%s}:lock"
        private const val PUBSUB_CHANNEL = "cb:pubsub"
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("CB-EventCoordinator"))

    @Value("classpath:scripts/lua/circuit-breaker/circuit_fsm.lua")
    private lateinit var luaFSMResource: Resource

    @Value("classpath:scripts/lua/lock/try_lock.lua")
    private lateinit var luaLockResource: Resource

    private lateinit var luaFSM: String
    private lateinit var luaLock: String

    // 외부 상태를 로컬에 반영하는 동안 로컬의 '전환 제안'을 억제하는 가드 (breaker별)
    private val suppressProposal = ConcurrentHashMap<String, AtomicBoolean>()

    // HALF_OPEN 카나리아 훅(선택)
    @Volatile
    var halfOpenProbe: (suspend (breakerName: String) -> Boolean)? = null

    @PostConstruct
    fun loadScriptsAndStart() {
        luaFSMResource.inputStream.use { luaFSM = it.readAllBytes().toString(StandardCharsets.UTF_8) }
        luaLockResource.inputStream.use { luaLock = it.readAllBytes().toString(StandardCharsets.UTF_8) }

        startPubSubListener()
        startBackupSyncScheduler()
        startHalfOpenScheduler()
    }

    @PreDestroy
    fun shutdown() {
        scope.cancel()
    }

    /** ================= 등록/부트스트랩 ================= */

    fun registerCircuitBreaker(entity: CircuitBreakerConfigEntity): Mono<Void> {
        if (cbRegistry.find(entity.breakerName).isPresent) return Mono.empty()

        val config = buildConfig(entity)
        val cb = cbRegistry.circuitBreaker(entity.breakerName, config)

        // Redis 비어있으면 기본 값으로 부트스트랩
        scope.launch { bootstrapIfAbsent(cb.name) }

        // 로컬 전환 이벤트는 '제안'만 수행. (외부 반영 중이면 억제)
        cb.eventPublisher.onStateTransition { evt ->
            val name = cb.name
            val from = evt.stateTransition.fromState.name
            val to = evt.stateTransition.toState.name
            scope.launch {
                if (suppressProposal[name]?.get() == true) return@launch
                runCatching { proposeTransitionSuspend(name, from, to) }
                    .onFailure { e -> log.warn("[CB FSM] proposal {} {}→{} failed: {}", name, from, to, e.message) }
            }
        }

        log.info("[CB INIT] Registered breaker: {}", entity.breakerName)
        return Mono.empty()
    }

    private fun buildConfig(e: CircuitBreakerConfigEntity): CircuitBreakerConfig {
        val failureRate = (e.failureRateThreshold ?: 50).toFloat()
        val slowRate = (e.slowCallRateThreshold ?: 100).toFloat()
        val slowDuration = Duration.ofMillis((e.slowCallDurationThresholdMs ?: 1000).toLong())
        val waitOpen = Duration.ofMillis((e.waitDurationInOpenStateMs ?: 60_000).toLong())
        val permittedHalfOpen = (e.permittedCallsInHalfOpenState ?: 1) // 단일 카나리아 권장
        val minCalls = e.minimumNumberOfCalls ?: 100
        val windowSize = e.slidingWindowSize ?: 100
        val windowType = runCatching {
            CircuitBreakerConfig.SlidingWindowType.valueOf(e.slidingWindowType ?: "COUNT_BASED")
        }.getOrDefault(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)

        return CircuitBreakerConfig.custom()
            .failureRateThreshold(failureRate)
            .slowCallRateThreshold(slowRate)
            .slowCallDurationThreshold(slowDuration)
            .waitDurationInOpenState(waitOpen)
            .permittedNumberOfCallsInHalfOpenState(permittedHalfOpen)
            .minimumNumberOfCalls(minCalls)
            .slidingWindowSize(windowSize)
            .slidingWindowType(windowType)
            .recordExceptions(
                YosepHttpErrorException::class.java,
                WebClientRequestException::class.java,
                java.net.SocketTimeoutException::class.java,
                TimeoutException::class.java,
            )
            .ignoreException { th ->
                if (th is YosepHttpErrorException) {
                    val st: HttpStatus? = th.httpStatus
                    st != null && !st.is5xxServerError
                } else false
            }
            .build()
    }

    private suspend fun bootstrapIfAbsent(name: String) {
        val key = KEY_STATE.format(name)
        val h = redisson.getMap<String, String>(key)
        val cur = h.get("state").awaitSingleOrNull()
        if (cur == null) {
            h.fastPut("state", "CLOSED").awaitSingle()
            h.fastPut("version", "0").awaitSingle()
            log.info("[CB BOOT] {} initialized to CLOSED@0", name)
        }
    }

    /** ================= FSM 제안(원자 전환) ================= */

    suspend fun proposeTransitionSuspend(name: String, from: String, to: String): Boolean {
        val key = KEY_STATE.format(name)
        val res = redisson.script.eval<Any>(
            RScript.Mode.READ_WRITE,
            luaFSM,
            RScript.ReturnType.VALUE,
            listOf(key),
            from, to
        ).awaitSingle()
        val json = res.toString()
        val node = om.readTree(json)
        val result = node.get("result")?.asText() ?: "?"
        val curState = node.get("currentState")?.asText()
        val curVer = node.get("currentVersion")?.asLong()
        return if (result != "OK") {
            log.warn("[CB FSM] {} proposal {}→{} rejected: {} (state={}, ver={})",
                name, from, to, result, curState, curVer)
            false
        } else {
            log.info("[CB FSM] {} {}→{} accepted @{}", name, from, to, curVer)
            true
        }
    }

    fun proposeTransition(name: String, from: String, to: String): Mono<Boolean> = mono {
        proposeTransitionSuspend(name, from, to)
    }

    /** ================= Pub/Sub 즉시 반영(제안 억제 포함) ================= */

    private fun startPubSubListener() {
        val topic: RTopicReactive = redisson.getTopic(PUBSUB_CHANNEL)
        topic.addListener(String::class.java) { _, msg ->
            scope.launch {
                runCatching { applyPubSubMessage(msg) }
                    .onFailure { e -> log.warn("[CB FSM] pubsub apply error: {}", e.message) }
            }
        }.subscribe()
        log.info("[CB FSM] PubSub listener started on '{}'", PUBSUB_CHANNEL)
    }

    private val nameKeyExtract: Pattern = Pattern.compile("cb:\\{(.+)}:state")

    private suspend fun applyPubSubMessage(msg: String) {
        val n = om.readTree(msg)
        val nameKey = n.get("nameKey")?.asText() ?: return
        val to = n.get("to")?.asText() ?: return
        val m = nameKeyExtract.matcher(nameKey)
        if (!m.matches()) return
        val name = m.group(1)

        withoutProposal(name) {
            val cb = cbRegistry.circuitBreaker(name)
            if (cb.state.name != to) {
                when (to) {
                    "OPEN" -> cb.transitionToOpenState()
                    "CLOSED" -> cb.transitionToClosedState()
                    "HALF_OPEN" -> cb.transitionToHalfOpenState()
                }
                log.info("[CB FSM] {} local state set to {} (via pubsub)", name, to)
            }
        }
    }

    /** ================= 백업 폴링(60s) ================= */

    private fun startBackupSyncScheduler() {
        scope.launch {
            while (isActive) {
                delay(60_000)
                try {
                    for (cb in cbRegistry.allCircuitBreakers) {
                        runCatching { syncFromRedisOncePublic(cb.name) }
                            .onFailure { e -> log.warn("[CB FSM] backup sync error {}: {}", cb.name, e.message) }
                    }
                } catch (_: Throwable) { /* noop */ }
            }
        }
    }

    /** ================= HALF_OPEN 카나리아 ================= */

    private fun startHalfOpenScheduler() {
        scope.launch {
            while (isActive) {
                try {
                    triggerHalfOpenCanaryOnce()
                } catch (e: Throwable) {
                    log.warn("[CB FSM] half-open scheduler error: {}", e.message)
                }
                delay(5_000)
            }
        }
    }

    suspend fun triggerHalfOpenCanaryOnce() {
        cbRegistry.allCircuitBreakers
            .filter { it.state == CircuitBreaker.State.HALF_OPEN }
            .forEach { cb ->
                val name = cb.name
                val lockJson = redisson.script.eval<Any>(
                    RScript.Mode.READ_WRITE,
                    luaLock,
                    RScript.ReturnType.VALUE,
                    listOf(KEY_LOCK.format(name)),
                    "5000"
                ).awaitSingle().toString()

                val acquired = runCatching {
                    val node = om.readTree(lockJson)
                    node.get("acquired")?.asBoolean() == true
                }.getOrElse { false }

                if (!acquired) return@forEach

                log.info("[CB FSM] {} HALF_OPEN canary lock acquired", name)

                val ok = try {
                    val probe = halfOpenProbe
                    if (probe != null) probe.invoke(name) else false
                } catch (_: Throwable) { false }

                if (ok) {
                    runCatching { proposeTransitionSuspend(name, "HALF_OPEN", "CLOSED") }
                        .onFailure { e -> log.warn("[CB FSM] {} canary->CLOSED propose failed: {}", name, e.message) }
                } else {
                    runCatching { proposeTransitionSuspend(name, "HALF_OPEN", "OPEN") }
                        .onFailure { e -> log.warn("[CB FSM] {} canary->OPEN propose failed: {}", name, e.message) }
                }
            }
    }

    /** ================= 초기 강제 싱크(부팅 직후 1회용) ================= */

    suspend fun syncFromRedisOncePublic(name: String) {
        val key = KEY_STATE.format(name)
        val m = redisson.getMap<String, String>(key)
        val state = m.get("state").awaitSingleOrNull() ?: return
        withoutProposal(name) {
            val cb = cbRegistry.circuitBreaker(name)
            if (cb.state.name != state) {
                when (state) {
                    "OPEN" -> cb.transitionToOpenState()
                    "CLOSED" -> cb.transitionToClosedState()
                    "HALF_OPEN" -> cb.transitionToHalfOpenState()
                }
                log.info("[CB FSM] {} local set to {} (initial sync)", name, state)
            }
        }
    }

    suspend fun initialSyncAllOnce() {
        for (cb in cbRegistry.allCircuitBreakers) {
            runCatching { syncFromRedisOncePublic(cb.name) }
                .onFailure { e -> log.warn("[CB FSM] initial sync error {}: {}", cb.name, e.message) }
        }
    }

    /** ================= 유틸 ================= */

    private inline fun <T> withoutProposal(name: String, block: () -> T): T {
        val flag = suppressProposal.computeIfAbsent(name) { AtomicBoolean(false) }
        val prev = flag.getAndSet(true)
        return try { block() } finally { flag.set(prev) }
    }

    fun getCircuitBreaker(name: String): CircuitBreaker = cbRegistry.circuitBreaker(name)
    fun getAll(): Set<CircuitBreaker> = cbRegistry.allCircuitBreakers
}

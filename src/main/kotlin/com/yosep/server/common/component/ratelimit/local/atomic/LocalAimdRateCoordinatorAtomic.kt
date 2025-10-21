package com.yosep.server.common.component.ratelimit.local.atomic

import com.yosep.server.common.component.k8s.MeshTopologyListener
import com.yosep.server.common.component.ratelimit.local.LocalRateLimitProperties
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import io.fabric8.kubernetes.client.KubernetesClient
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min

/**
 * p99(시간 윈도우 기반) AIMD Rate Coordinator
 *
 * - 각 org의 최근 latency 샘플을 (latency, tsMs)로 고정 크기 ring buffer에 기록
 * - 매 주기 p99(최근 windowMs 내 샘플만) 계산해 limit 증감(AIMD)
 * - onSuccess/onFailure는 latency만 기록(가벼움). 필요 시 외부 tsMs 주입 가능
 */
@Component
@ConditionalOnProperty(
    prefix = "ratelimit.local.atomic",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = false
)
class LocalAimdRateCoordinatorAtomic(
    val k8s: KubernetesClient,
    private val properties: LocalRateLimitProperties,
    @Value("\${mesh.service:finda-mydata-external-server}") private val meshSvc: String,
    @Value("\${ratelimit.local.atomic.totalTargetQps:0}") private val totalTargetQps: Int,
): MeshTopologyListener {
    private val factory = k8s.informers()
    private val logger = LoggerFactory.getLogger(javaClass)

    // ---- Tunables (properties 없을 때 기본값) ----
    private val maxLimit get() = properties.maxLimit
    private val minLimit get() = properties.minLimit
    private val targetP99Ms get() = (properties.targetP99Ms.takeIf { it > 0 } ?: 500L)
    private val addStep get() = (properties.addStep.takeIf { it > 0 } ?: 50)
    private val decreaseFactor get() = (properties.decreaseFactor.takeIf { it in 0.0..1.0 } ?: 0.7)
    private val reservoirSize get() = (properties.reservoirSize.takeIf { it > 0 } ?: 10000)
    private val latencyWindowMs get() = (properties.latencyWindowMs.takeIf { it > 0 } ?: 1000L) // 기본 1초
    private val minSamplesForP99 get() = (properties.minSamplesForP99.takeIf { it > 0 } ?: 64)

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val coordinatorStates = ConcurrentHashMap<String, AtomicReference<CoordinatorState>>()
    private val reservoirs = ConcurrentHashMap<String, LatencyReservoir>()
    private val lastAdjustMs = ConcurrentHashMap<String, AtomicLong>()

    // ready 파드 수 & per-pod cap
    private val readyPods = AtomicInteger(1)
    private val perPodCap = AtomicInteger(maxLimit)

    @PostConstruct
    fun init() {
        startPeriodicAdjustment(latencyWindowMs)
    }

    companion object {
        const val UPDATE_INTERVAL_MS = 1_000L
    }

    data class CoordinatorState(
        val limit: Int,
        val lastP99: Long = 0L,
        val version: Long = System.nanoTime()
    )

    /** 🔸 BRolloutHealthyNotifier가 호출하는 "세팅 메서드" */
    override fun onReadyEndpointsChanged(service: String, endpoints: Set<String>) {
        if (service != meshSvc) return
        val count = max(1, endpoints.size)
        readyPods.set(count)

        val cap =
            if (totalTargetQps > 0) max(minLimit, totalTargetQps / count)
            else maxLimit

        perPodCap.set(cap)
        logger.info("[AIMD] onReadyEndpointsChanged: service={}, readyPods={}, perPodCap={}", service, count, cap)
    }

    override fun onRolloutHealthy(rolloutName: String) {
        logger.info("[AIMD] rollout healthy: {}", rolloutName)
        // 필요하면 여기서 내부 상태 리셋/로그 남기기 등
    }

    /** 외부에서 현재 유효 limit을 가져갈 때 cap 적용 (예) */
//    fun getCurrentLimit(org: String): Int {
//        val currentRaw = /* 기존 상태에서 raw limit 조회 */ maxLimit
//        return min(currentRaw, perPodCap.get())
//    }

    // ---- 초기 설정 로드 ----
    suspend fun initializeFromConfig(configs: List<OrgRateLimitConfigEntity>) {
        configs.forEach { cfg ->
            val org = cfg.id
            coordinatorStates.computeIfAbsent(org) {
                AtomicReference(CoordinatorState(limit = cfg.initialQps.coerceIn(minLimit, maxLimit)))
            }
            reservoirs.computeIfAbsent(org) { LatencyReservoir(reservoirSize) }
            lastAdjustMs.computeIfAbsent(org) { AtomicLong(System.currentTimeMillis()) }
        }
        logger.info(
            "[AIMD-p99] Initialized {} orgs (reservoir={}, window={}ms, targetP99={}ms, addStep={}, decFactor={}, minSamples={})",
            configs.size, reservoirSize, latencyWindowMs, targetP99Ms, addStep, decreaseFactor, minSamplesForP99
        )
    }

    // ---- 샘플 기록 (성공/실패 공통) ----
    suspend fun onSuccess(org: String, latencyMs: Long, tsMs: Long = System.currentTimeMillis()) {
        reservoirs.computeIfAbsent(org) { LatencyReservoir(reservoirSize) }.record(latencyMs, tsMs)
    }

    suspend fun onFailure(org: String, latencyMs: Long, tsMs: Long = System.currentTimeMillis()) {
        reservoirs.computeIfAbsent(org) { LatencyReservoir(reservoirSize) }.record(latencyMs, tsMs)
    }

    // ---- 현재 Limit/State 조회 ----
    fun getCurrentLimit(org: String): Int =
        coordinatorStates[org]?.get()?.limit ?: maxLimit

    fun getState(org: String): CoordinatorState? =
        coordinatorStates[org]?.get()

    // ---- 주기적 조정 시작 (윈도우 오버라이드 가능) ----
    fun startPeriodicAdjustment(windowMs: Long = latencyWindowMs) {
        scope.launch {
            while (isActive) {
                delay(UPDATE_INTERVAL_MS)
                adjustAll(windowMs)
            }
        }
    }

    // 필요 시 수동 1회 조정
    suspend fun adjustOnce(windowMs: Long = latencyWindowMs) {
        adjustAll(windowMs)
    }

    private suspend fun adjustAll(windowMs: Long) {
        val now = System.currentTimeMillis()
        reservoirs.forEach { (org, resv) ->
            val p99 = resv.p99Within(windowMs, now, minSamplesForP99) ?: return@forEach
            val stateRef = coordinatorStates.computeIfAbsent(org) { AtomicReference(CoordinatorState(limit = maxLimit)) }

            stateRef.updateAndGet { cur ->
                val oldLimit = cur.limit
                val newLimit =
                    if (p99 <= targetP99Ms) {
                        // Additive Increase
                        min(maxLimit, oldLimit + addStep)
                    } else {
                        // Multiplicative Decrease
                        max(minLimit, (oldLimit * decreaseFactor).toInt().coerceAtLeast(minLimit))
                    }

                if (logger.isDebugEnabled) {
                    logger.debug(
                        "[AIMD-p99] org={}, p99={}ms (window={}ms), target={}ms, limit: {} -> {}",
                        org, p99, windowMs, targetP99Ms, oldLimit, newLimit
                    )
                }

                cur.copy(limit = newLimit, lastP99 = p99, version = System.nanoTime())
            }

            lastAdjustMs[org]?.set(now)
        }
    }

    // ---- 통계 ----
    fun getStatistics(): Map<String, Any> {
        val orgs = coordinatorStates.mapValues { (_, ref) ->
            val st = ref.get()
            mapOf(
                "limit" to st.limit,
                "lastP99Ms" to st.lastP99
            )
        }
        return mapOf("organizations" to orgs, "totalOrgs" to orgs.size)
    }

    // ---- 종료 ----
    fun shutdown() {
        scope.cancel()
        logger.info("[AIMD-p99] Coordinator shutdown completed")
    }

    /**
     * 고정 크기 ring buffer (lock-free writes, 안전한 게시 스탬프)
     * - values[i], times[i]를 쓰고 나서 stamps[i]로 '게시 완료' 표시
     * - 읽기는 stamps 기준으로 최근 cap개 중 windowMs 내의 샘플만 수집
     */
    private class LatencyReservoir(capacity: Int) {
        private val cap = capacity
        private val values = LongArray(cap)
        private val times = LongArray(cap)
        private val seq = AtomicLong(0) // 총 게시 시퀀스(증가만)
        private val stamps = java.util.concurrent.atomic.AtomicLongArray(cap)
        // stamps[i] == 0        : 미게시
        // stamps[i] == k(>=1)   : 시퀀스 k로 게시 완료

        fun record(latencyMs: Long, tsMs: Long = System.currentTimeMillis()) {
            val slotSequence = seq.getAndIncrement()           // 슬롯 시퀀스(게시 번호)
            val slotIndex = (slotSequence % cap).toInt()
            values[slotIndex] = latencyMs                   // 1) 값
            times[slotIndex] = tsMs                         // 2) 타임스탬프
            stamps.set(slotIndex, slotSequence + 1)                    // 3) 게시 완료 표시 (퍼블리시)
        }

        fun size(): Int = min(seq.get().toInt(), cap)

        fun p99Within(windowMs: Long, nowMs: Long = System.currentTimeMillis(), minSamples: Int = 0): Long? {
            val published = seq.get()
            if (published == 0L) return null
            val lowerBound = (published - cap).coerceAtLeast(0) // 최근 cap개만 고려

            // window 내 샘플만 임시 배열에 모아 정렬
            val snap = LongArray(cap)
            var n = 0
            for (i in 0 until cap) {
                val s = stamps.get(i)
                if (s > lowerBound) {
                    val ts = times[i]
                    if (nowMs - ts <= windowMs) {
                        snap[n++] = values[i]
                    }
                }
            }
            if (n < max(1, minSamples)) return null
            java.util.Arrays.sort(snap, 0, n)
            val idx = floor((n - 1) * 0.99).toInt().coerceIn(0, n - 1)
            return snap[idx]
        }
    }
}

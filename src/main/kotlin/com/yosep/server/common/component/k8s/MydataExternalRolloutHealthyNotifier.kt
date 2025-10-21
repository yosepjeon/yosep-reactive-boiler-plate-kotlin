package com.yosep.server.common.component.k8s

// ... (기존 import 동일)
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSlice
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Profile
import org.springframework.context.event.EventListener
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

@Component
@Profile("prd")
class MydataExternalRolloutHealthyNotifier(
    private val k8s: KubernetesClient,
    private val defaultWebClient: WebClient,
    private val listeners: List<MeshTopologyListener>,
    @Value("\${rollout.namespace}") private val ns: String,
    @Value("\${rollout.rolloutName}") private val rolloutName: String,
    @Value("\${rollout.serviceName}") private val svcName: String,
    @Value("\${rollout.portName:http}") private val portName: String,
    @Value("\${rollout.slackWebhook:}") private val slackWebhook: String
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val debouncing = AtomicBoolean(false)

    private lateinit var crdCtx: CustomResourceDefinitionContext
    @Volatile private var healthyNotified = false

    // ▶ watch 핸들 + 백오프 상태
    private var rolloutWatch: Watch? = null
    private val reconnectAttempts = AtomicLong(0)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        // 1) Rollout CRD 버전 자동 감지
        val crd = k8s.apiextensions().v1().customResourceDefinitions()
            .withName("rollouts.argoproj.io").get()
        val version = crd?.spec?.versions?.firstOrNull { it.storage == true }?.name
            ?: crd?.spec?.versions?.firstOrNull { it.served == true }?.name
            ?: "v1alpha1"

        crdCtx = CustomResourceDefinitionContext.Builder()
            .withGroup("argoproj.io").withVersion(version)
            .withScope("Namespaced").withPlural("rollouts").build()
        log.info("Using Rollout CRD version: {}", version)

        // 2) watch 시작 (자동 재연결 포함)
        ensureRolloutWatch()

        // 3) 주기적 보정: Healthy를 놓쳤는지 검증 + Ready 엔드포인트 방송
        scope.launch {
            while (isActive) {
                delay(15_000) // 보정 주기
                try {
                    // Healthy 보정
                    val cur = k8s.genericKubernetesResources(crdCtx)
                        .inNamespace(ns).withName(rolloutName).get()
                    if (!healthyNotified && cur != null && isHealthy(cur)) {
                        onHealthy()
                    }
                } catch (e: Exception) {
                    log.debug("rollout poll failed: ${e.message}")
                }

                // Ready 엔드포인트 스냅샷 방송
                broadcastReadyEndpoints()
            }
        }
    }

    /** ▶ 끊기면 자동 재구독(backoff) */
    private fun ensureRolloutWatch() {
        rolloutWatch?.close()
        rolloutWatch = k8s.genericKubernetesResources(crdCtx)
            .inNamespace(ns).withName(rolloutName)
            .watch(object : Watcher<GenericKubernetesResource> {
                override fun eventReceived(a: Watcher.Action, res: GenericKubernetesResource) {
                    reconnectAttempts.set(0) // 이벤트 받았으면 성공으로 간주
                    if (!healthyNotified && isHealthy(res)) {
                        onHealthy()
                    }
                }
                override fun onClose(cause: WatcherException?) {
                    log.warn("Rollout watch closed: ${cause?.message}")
                    scope.launch {
                        // 지수 백오프 후 재연결
                        val attempt = reconnectAttempts.incrementAndGet()
                        val backoffMs = min(60_000L, (1_000L * (1 shl attempt.toInt().coerceAtMost(10))))
                        delay(backoffMs)
                        ensureRolloutWatch()
                    }
                }
            })
    }

    /** ▶ Healthy 전환 시 한 번만 처리 */
    private fun onHealthy() {
        healthyNotified = true
        val msg = "✅ Rollout Healthy: $ns/$rolloutName"
        println(msg)
        if (slackWebhook.isNotBlank()) {
            defaultWebClient.post().uri(slackWebhook)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(mapOf("text" to msg))
                .retrieve().toBodilessEntity()
                .subscribe({}, { e -> log.warn("Slack notify failed: ${e.message}") })
        }
        listeners.forEach { it.onRolloutHealthy(rolloutName) }
        broadcastReadyEndpoints()
    }

    private fun isHealthy(res: GenericKubernetesResource): Boolean {
        val st = res.additionalProperties["status"] as? Map<*, *> ?: return false
        val phase = st["phase"] as? String ?: return false
        if (phase != "Healthy") return false
        val spec = res.additionalProperties["spec"] as? Map<*, *> ?: return true
        val desired = (spec["replicas"] as? Number)?.toInt() ?: return true
        val updated = (st["updatedReplicas"] as? Number)?.toInt() ?: return true
        val available = (st["availableReplicas"] as? Number)?.toInt() ?: return true
        return (updated == desired && available == desired)
    }

    /** Ready host:port 계산해서 리스너들에게 전달 */
    private fun broadcastReadyEndpoints() {
        if (!debouncing.compareAndSet(false, true)) return
        scope.launch {
            try {
                val addrs = currentReadyHostPorts(ns, svcName, portName)
                listeners.forEach { it.onReadyEndpointsChanged(svcName, addrs) }
            } finally {
                delay(200) // 디바운스
                debouncing.set(false)
            }
        }
    }

    private fun currentReadyHostPorts(ns: String, svc: String, portName: String): Set<String> {
        val slices = k8s.resources(EndpointSlice::class.java)
            .inNamespace(ns)
            .withLabel("kubernetes.io/service-name", svc)
            .list().items.orEmpty()

        return slices.flatMap { sl ->
            val port = sl.ports?.firstOrNull { it.name == portName }?.port
                ?: sl.ports?.firstOrNull()?.port
                ?: return@flatMap emptyList<String>()
            sl.endpoints.orEmpty()
                .filter { it.conditions?.ready == true }
                .flatMap { ep ->
                    ep.addresses.orEmpty().map { ip ->
                        if (ip.contains(":")) "[$ip]:$port" else "$ip:$port"
                    }
                }
        }.toSet()
    }
}

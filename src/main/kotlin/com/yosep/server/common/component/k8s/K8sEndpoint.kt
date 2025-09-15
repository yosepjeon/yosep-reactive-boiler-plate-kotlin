package com.yosep.server.common.component.k8s

data class ObjectMeta(val name: String, val namespace: String)
data class EndpointAddress(val ip: String, val hostname: String? = null)
data class EndpointPort(val name: String? = null, val port: Int, val protocol: String = "TCP")
data class EndpointsSubset(
    val addresses: List<EndpointAddress> = emptyList(),
    val ports: List<EndpointPort> = emptyList()
)
data class K8sEndpointsLike(
    val apiVersion: String = "v1",
    val kind: String = "Endpoints",
    val metadata: ObjectMeta,
    val subsets: List<EndpointsSubset> = emptyList()
)

/** Endpoints 조회 추상화 (로컬/실서버 모두 동일하게 사용) */
interface EndpointsProvider {
    fun get(namespace: String, serviceName: String): K8sEndpointsLike?
}
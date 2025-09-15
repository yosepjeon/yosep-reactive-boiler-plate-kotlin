package com.yosep.server.common.component.k8s

import io.fabric8.kubernetes.client.KubernetesClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile("prd")
@Component
class RealK8sEndpointsProvider(
    private val k8s: KubernetesClient,
    @Value("\${cb.k8s.namespace:default}") private val defaultNs: String
) : EndpointsProvider {
    override fun get(namespace: String, serviceName: String): K8sEndpointsLike? {
        val ep = k8s.endpoints().inNamespace(namespace.ifBlank { defaultNs }).withName(serviceName).get() ?: return null
        val subsets = ep.subsets.orEmpty().map { ss ->
            EndpointsSubset(
                addresses = ss.addresses.orEmpty().map { a -> EndpointAddress(ip = a.ip, hostname = a.hostname) },
                ports = ss.ports.orEmpty()
                    .map { p -> EndpointPort(name = p.name, port = p.port, protocol = p.protocol ?: "TCP") }
            )
        }
        return K8sEndpointsLike(
            metadata = ObjectMeta(name = ep.metadata.name, namespace = ep.metadata.namespace),
            subsets = subsets
        )
    }
}
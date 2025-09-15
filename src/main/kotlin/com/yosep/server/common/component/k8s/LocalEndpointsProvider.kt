package com.yosep.server.common.component.k8s

import com.yosep.server.common.component.k8s.properties.LocalPeerProps
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.net.InetAddress
import java.net.URI

@Profile("!prd")
@Component
class LocalEndpointsProvider(
    private val props: LocalPeerProps
) : EndpointsProvider {

    override fun get(namespace: String, serviceName: String): K8sEndpointsLike? {
        if (props.peers.isEmpty()) return null

        // peers 예: http://localhost:8081, http://127.0.0.1:8082
        val subsets = props.peers.mapNotNull { p ->
            runCatching {
                val u = URI(p)
                val host = u.host ?: return@runCatching null
                val ip = InetAddress.getByName(host).hostAddress
                val port = if (u.port > 0) u.port else 8080
                EndpointsSubset(
                    addresses = listOf(EndpointAddress(ip = ip, hostname = host)),
                    ports = listOf(EndpointPort(name = props.portName, port = port))
                )
            }.getOrNull()
        }

        return K8sEndpointsLike(
            metadata = ObjectMeta(name = serviceName, namespace = namespace),
            subsets = subsets
        )
    }
}

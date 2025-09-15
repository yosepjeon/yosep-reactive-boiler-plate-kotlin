package com.yosep.server.common.component.k8s.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile("!prd")
@Component
@ConfigurationProperties("cb.local")
data class LocalPeerProps(
    var peers: List<String> = emptyList(),
    var namespace: String = "default",
    var serviceName: String = "cb-app",
    var portName: String = "http"
)

package com.yosep.server.common.config

import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("prd")
class K8sClientConfig {

    @Value("\${cb.k8s.master-url:}")
    private val masterUrl: String? = null

    @Value("\${cb.k8s.namespace:default}")
    private val namespace: String = "default"

    @Value("\${cb.k8s.trust-certs:true}")
    private val trustCerts: Boolean = true

    @Value("\${cb.k8s.connection-timeout:10000}")
    private val connectionTimeout: Int = 10000

    @Value("\${cb.k8s.request-timeout:10000}")
    private val requestTimeout: Int = 10000

    @Bean
    fun kubernetesClient(): KubernetesClient {
        val configBuilder = ConfigBuilder()
            .withNamespace(namespace)
            .withTrustCerts(trustCerts)
            .withConnectionTimeout(connectionTimeout)
            .withRequestTimeout(requestTimeout)

        // 마스터 URL이 지정되면 사용, 아니면 클러스터 내부에서 자동 감지
        if (!masterUrl.isNullOrBlank()) {
            configBuilder.withMasterUrl(masterUrl)
        }

        val config: Config = configBuilder.build()
        return KubernetesClientBuilder().withConfig(config).build()
    }
}
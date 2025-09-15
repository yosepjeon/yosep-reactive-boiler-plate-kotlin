package com.yosep.server.common.config

import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.mockk
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("!prd")
class MockK8sClientConfig {

    /**
     * 로컬 환경에서는 실제 K8s 클러스터가 없으므로 Mock Client를 제공
     * 실제로는 사용되지 않지만 Bean 주입 오류를 방지하기 위함
     * LocalEndpointsProvider가 사용되므로 실제로 K8s API 호출은 발생하지 않음
     */
    @Bean
    @ConditionalOnMissingBean(KubernetesClient::class)
    fun kubernetesClient(): KubernetesClient {
        // MockK를 사용한 완전한 Mock 객체
        return mockk<KubernetesClient>(relaxed = true)
    }
}
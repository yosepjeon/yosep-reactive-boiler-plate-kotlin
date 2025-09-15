package com.yosep.server.config

import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.mockk
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile

@TestConfiguration
@Profile("test")
class TestK8sConfig {
    
    @Bean
    @Primary
    fun testKubernetesClient(): KubernetesClient {
        // 테스트용 Mock KubernetesClient
        return mockk<KubernetesClient>(relaxed = true)
    }
}
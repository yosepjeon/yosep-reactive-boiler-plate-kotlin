package com.yosep.server.common.component.ratelimit.local

import com.yosep.server.common.component.k8s.*
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.test.util.ReflectionTestUtils

class NodeCountBasedLimitCalculatorTest {

    private lateinit var endpointsProvider: EndpointsProvider
    private lateinit var calculator: NodeCountBasedLimitCalculator

    @BeforeEach
    fun setUp() {
        endpointsProvider = mockk()
        calculator = NodeCountBasedLimitCalculator(endpointsProvider)
        
        // 캐시 시간을 0으로 설정하여 매번 새로 조회하도록 함
        ReflectionTestUtils.setField(calculator, "nodeCountCacheMs", 0L)
        ReflectionTestUtils.setField(calculator, "minPerNode", 1)
    }

    @Test
    @DisplayName("노드 수 기반 제한 계산 - 정상 케이스")
    fun testCalculatePerNodeLimitNormal() {
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = listOf(
                        EndpointAddress("10.0.1.1"),
                        EndpointAddress("10.0.1.2"),
                        EndpointAddress("10.0.1.3")
                    ),
                    ports = listOf(EndpointPort(port = 8080))
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val totalLimit = 1500
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(500) // 1500 / 3 = 500
        assertThat(calculator.getNodeCount()).isEqualTo(3)
    }

    @Test
    @DisplayName("노드 수 기반 제한 계산 - 단일 노드")
    fun testCalculatePerNodeLimitSingleNode() {
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = listOf(EndpointAddress("10.0.1.1")),
                    ports = listOf(EndpointPort(port = 8080))
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val totalLimit = 1000
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(1000) // 1000 / 1 = 1000
        assertThat(calculator.getNodeCount()).isEqualTo(1)
    }

    @Test
    @DisplayName("노드 수 기반 제한 계산 - endpoints가 없는 경우")
    fun testCalculatePerNodeLimitNoEndpoints() {
        every { endpointsProvider.get(any(), any()) } returns null

        val totalLimit = 2000
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(2000) // 기본 노드 수 1로 계산
        assertThat(calculator.getNodeCount()).isEqualTo(1)
    }

    @Test
    @DisplayName("노드 수 기반 제한 계산 - 빈 addresses")
    fun testCalculatePerNodeLimitEmptyAddresses() {
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = emptyList(),
                    ports = listOf(EndpointPort(port = 8080))
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val totalLimit = 3000
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(3000) // 기본 노드 수 1로 계산
        assertThat(calculator.getNodeCount()).isEqualTo(1)
    }

    @Test
    @DisplayName("최소 노드당 제한 보장")
    fun testMinPerNodeLimit() {
        ReflectionTestUtils.setField(calculator, "minPerNode", 5)
        
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = listOf(
                        EndpointAddress("10.0.1.1"),
                        EndpointAddress("10.0.1.2"),
                        EndpointAddress("10.0.1.3"),
                        EndpointAddress("10.0.1.4"),
                        EndpointAddress("10.0.1.5")
                    )
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val totalLimit = 10 // 매우 작은 제한으로 설정
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(5) // minPerNode 보장
        assertThat(calculator.getNodeCount()).isEqualTo(5)
    }

    @Test
    @DisplayName("예외 발생 시 기본값 사용")
    fun testExceptionFallback() {
        every { endpointsProvider.get(any(), any()) } throws RuntimeException("K8s API 오류")

        val totalLimit = 1000
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(1000) // 기본 노드 수 1로 계산
        assertThat(calculator.getNodeCount()).isEqualTo(1)
    }

    @Test
    @DisplayName("강제 새로고침 테스트")
    fun testForceRefresh() {
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = listOf(
                        EndpointAddress("10.0.1.1"),
                        EndpointAddress("10.0.1.2")
                    )
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val nodeCount = calculator.forceRefreshNodeCount()
        assertThat(nodeCount).isEqualTo(2)
        assertThat(calculator.getNodeCount()).isEqualTo(2)
    }

    @Test
    @DisplayName("여러 서브셋 처리")
    fun testMultipleSubsets() {
        val endpoints = K8sEndpointsLike(
            metadata = ObjectMeta(name = "test-service", namespace = "default"),
            subsets = listOf(
                EndpointsSubset(
                    addresses = listOf(
                        EndpointAddress("10.0.1.1"),
                        EndpointAddress("10.0.1.2")
                    )
                ),
                EndpointsSubset(
                    addresses = listOf(
                        EndpointAddress("10.0.2.1"),
                        EndpointAddress("10.0.2.2"),
                        EndpointAddress("10.0.2.3")
                    )
                )
            )
        )
        every { endpointsProvider.get(any(), any()) } returns endpoints

        val totalLimit = 2500
        val perNodeLimit = calculator.calculatePerNodeLimit(totalLimit)

        assertThat(perNodeLimit).isEqualTo(500) // 2500 / 5 = 500
        assertThat(calculator.getNodeCount()).isEqualTo(5) // 2 + 3 = 5
    }
}
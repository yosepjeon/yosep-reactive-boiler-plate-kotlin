package com.yosep.server.common.service.circuitbreaker

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.write.repository.CircuitBreakerConfigWriteRepository
import com.yosep.server.infrastructure.db.common.write.repository.OrgInfoWriteRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactiveCircuitBreakerServiceTest @Autowired constructor(
    private val service: ReactiveCircuitBreakerService,
    private val circuitBreakerConfigWriteRepository: CircuitBreakerConfigWriteRepository,
    private val orgInfoWriteRepository: OrgInfoWriteRepository,
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `initializeCircuitBreakers는 멱등적이며 기존 항목을 건너뛴다`() = runTest {
        // circuit_breaker_config 초기화(깨끗한 상태 보장)
        circuitBreakerConfigWriteRepository.deleteAll()

        val distinctOrgCount = orgInfoWriteRepository.findAll().toList()
            .mapNotNull { it.orgCode }
            .toSet()
            .size

        // 1차 실행: 모든 org code에 대해 엔트리 생성
        service.initializeCircuitBreakers()
        val countAfterFirst = circuitBreakerConfigWriteRepository.findAll().toList().size
        assertEquals(distinctOrgCount, countAfterFirst, "First init should create one config per org")

        // 2차 실행: 중복 생성 없이 카운트 동일해야 함
        service.initializeCircuitBreakers()
        val countAfterSecond = circuitBreakerConfigWriteRepository.findAll().toList().size
        assertEquals(distinctOrgCount, countAfterSecond, "Second init should not create duplicates")
    }
}

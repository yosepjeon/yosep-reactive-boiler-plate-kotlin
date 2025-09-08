package com.yosep.server.common.service.circuitbreaker

import com.yosep.server.common.component.circuitbreaker.ReactiveRedisCircuitBreakerEventCoordinator
import com.yosep.server.common.util.circuitbreaker.CircuitBreakerConfigEntityGenerator
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.CircuitBreakerConfigWriteRepository
import com.yosep.server.infrastructure.db.common.write.repository.OrgInfoWriteRepository
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait

@Service
class ReactiveCircuitBreakerService(
    private val orgInfoWriteRepository: OrgInfoWriteRepository,
    private val coordinator: ReactiveRedisCircuitBreakerEventCoordinator,
    private val circuitBreakerConfigWriteRepository: CircuitBreakerConfigWriteRepository,
    private val masterTx: TransactionalOperator,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ReactiveCircuitBreakerService"))

    @PostConstruct
    fun init() {
        scope.launch {
            // 1) 모두 등록
            val list = initializeCircuitBreakers()
            // 2) 부팅 직후 1회 강제 동기화 (Pub/Sub 이벤트가 없더라도 즉시 동기화됨)
            runCatching { coordinator.initialSyncAllOnce() }
                .onFailure { /* 부팅시 오류는 로그로만 */ }
        }
    }

    @PreDestroy
    fun shutdown() {
        scope.cancel()
    }

    fun getCircuitBreaker(name: String): CircuitBreaker = coordinator.getCircuitBreaker(name)

    suspend fun initializeCircuitBreakers(): List<CircuitBreakerConfigEntity> =
        masterTx.executeAndAwait {
            val orgCodes = orgInfoWriteRepository.findAll()
                .toList()
                .mapNotNull { it.orgCode }
                .toSet()

            val result = mutableListOf<CircuitBreakerConfigEntity>()
            for (orgCode in orgCodes) {
                val breakerName = "$orgCode-mydata"
                val existing = circuitBreakerConfigWriteRepository.findByBreakerName(breakerName)
                val entity = existing ?: circuitBreakerConfigWriteRepository.save(
                    CircuitBreakerConfigEntityGenerator.defaultConfig(breakerName)
                )
                result += entity
            }

            // 로컬 등록 (부트스트랩/동기화는 코디네이터가 처리)
            result.forEach { coordinator.registerCircuitBreaker(it).awaitSingleOrNull() }
            result
        }

    // 선택: 운영 툴/디버깅용 수동 싱크(일회성)
    suspend fun syncSingleCircuitBreakerFromRedis(name: String) {
        coordinator.syncFromRedisOncePublic(name)
    }

    fun getAllCircuitBreakers(): Set<CircuitBreaker> = coordinator.getAll()

    suspend fun clearAllCircuitBreakers() =
        masterTx.executeAndAwait { circuitBreakerConfigWriteRepository.deleteAll() }

    suspend fun deleteAllCircuitBreakers() =
        masterTx.executeAndAwait { circuitBreakerConfigWriteRepository.deleteAll() }
}

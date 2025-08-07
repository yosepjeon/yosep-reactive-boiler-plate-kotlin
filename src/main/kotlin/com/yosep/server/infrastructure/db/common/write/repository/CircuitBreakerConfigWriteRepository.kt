package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface CircuitBreakerConfigWriteRepository : CoroutineCrudRepository<CircuitBreakerConfigEntity, String> {
    suspend fun findByBreakerName(breakerName: String): CircuitBreakerConfigEntity?
}
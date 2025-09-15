package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface CircuitBreakerConfigWriteRepository : CoroutineCrudRepository<CircuitBreakerConfigEntity, String> {
    @Query("SELECT * FROM circuit_breaker_config WHERE breaker_name = :breakerName LIMIT 1")
    suspend fun findByBreakerName(breakerName: String): CircuitBreakerConfigEntity?
}
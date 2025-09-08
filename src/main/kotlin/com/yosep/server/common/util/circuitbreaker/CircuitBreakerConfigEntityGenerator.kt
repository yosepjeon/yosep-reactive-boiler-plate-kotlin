package com.yosep.server.common.util.circuitbreaker

import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import java.time.LocalDateTime

object CircuitBreakerConfigEntityGenerator {
    fun defaultConfig(breakerName: String): CircuitBreakerConfigEntity {
        return CircuitBreakerConfigEntity(
            breakerName = breakerName,
            failureRateThreshold = 20,
            slowCallRateThreshold = 20,
            slowCallDurationThresholdMs = 60_000,   // 60s
            waitDurationInOpenStateMs = 30_000,     // 30s
            permittedCallsInHalfOpenState = 10,
            minimumNumberOfCalls = 10,
            slidingWindowSize = 100,
            slidingWindowType = "COUNT_BASED",
            recordFailureStatusCodes = null,        // 필요시 문자열로 "400,500" 등 저장
            createdAt = LocalDateTime.now(),
            updatedAt = LocalDateTime.now()
        ).markNew()
    }
}
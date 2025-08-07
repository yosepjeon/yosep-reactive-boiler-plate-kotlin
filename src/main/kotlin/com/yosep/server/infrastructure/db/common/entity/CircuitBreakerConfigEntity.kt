package com.yosep.server.infrastructure.db.common.entity

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("circuit_breaker_config")
data class CircuitBreakerConfigEntity(
    @Id
    @Column("breaker_name")
    val breakerName: String,

    @Column("failure_rate_threshold")
    val failureRateThreshold: Int?,

    @Column("slow_call_rate_threshold")
    val slowCallRateThreshold: Int?,

    @Column("slow_call_duration_threshold_ms")
    val slowCallDurationThresholdMs: Int?,

    @Column("wait_duration_in_open_state_ms")
    val waitDurationInOpenStateMs: Int?,

    @Column("permitted_calls_in_half_open_state")
    val permittedCallsInHalfOpenState: Int?,

    @Column("minimum_number_of_calls")
    val minimumNumberOfCalls: Int?,

    @Column("sliding_window_size")
    val slidingWindowSize: Int?,

    @Column("sliding_window_type")
    val slidingWindowType: String?,

    @Column("record_failure_status_codes")
    val recordFailureStatusCodes: String?,

    @Column("insert_time")
    val createdAt: LocalDateTime?,

    @Column("update_time")
    val updatedAt: LocalDateTime?,
) : Persistable<String> {

    @Transient
    private var isNew: Boolean = false

    constructor(
        breakerName: String,
        failureRateThreshold: Int?,
        slowCallRateThreshold: Int?,
        slowCallDurationThresholdMs: Int?,
        waitDurationInOpenStateMs: Int?,
        permittedCallsInHalfOpenState: Int?,
        minimumNumberOfCalls: Int?,
        slidingWindowSize: Int?,
        slidingWindowType: String?,
        recordFailureStatusCodes: String?,
        createdAt: LocalDateTime?,
        updatedAt: LocalDateTime?,
        isNew: Boolean
    ) : this(
        breakerName,
        failureRateThreshold,
        slowCallRateThreshold,
        slowCallDurationThresholdMs,
        waitDurationInOpenStateMs,
        permittedCallsInHalfOpenState,
        minimumNumberOfCalls,
        slidingWindowSize,
        slidingWindowType,
        recordFailureStatusCodes,
        createdAt,
        updatedAt
    ) {
        this.isNew = isNew
    }

    override fun getId(): String = breakerName

    override fun isNew(): Boolean = isNew

    fun markNew(): CircuitBreakerConfigEntity {
        this.isNew = true
        return this
    }
}
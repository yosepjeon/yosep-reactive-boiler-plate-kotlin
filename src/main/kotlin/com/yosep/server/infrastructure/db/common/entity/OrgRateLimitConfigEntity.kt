package com.yosep.server.infrastructure.db.common.entity

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("org_rate_limit_config")
data class OrgRateLimitConfigEntity private constructor(
    @Id
    @Column("id")
    private val id: String,

    @Column("initial_qps")
    val initialQps: Int,

    @Column("max_qps")
    val maxQps: Int,

    @Column("min_qps")
    val minQps: Int,

    @Column("latency_threshold")
    val latencyThreshold: Int,

    @Column("created_at")
    val createdAt: LocalDateTime?,

    @Column("updated_at")
    val updatedAt: LocalDateTime?
) : Persistable<String> {

    @Transient
    private var isNew: Boolean = false

    constructor(
        id: String,
        initialQps: Int,
        maxQps: Int,
        minQps: Int,
        latencyThreshold: Int,
        createdAt: LocalDateTime?,
        updatedAt: LocalDateTime?,
        isNew: Boolean
    ) : this(id, initialQps, maxQps, minQps, latencyThreshold, createdAt, updatedAt) {
        this.isNew = isNew
    }

    override fun getId(): String = id

    override fun isNew(): Boolean = isNew

    fun markNew(): OrgRateLimitConfigEntity {
        this.isNew = true
        return this
    }
}

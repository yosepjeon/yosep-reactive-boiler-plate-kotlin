package com.yosep.server.infrastructure.db.common.entity

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("webclient_config")
data class WebClientConfigEntity private constructor(
    @Id
    private val id: String,

    @Column("base_url")
    val baseUrl: String?,

    @Column("connect_timeout_ms")
    val connectTimeoutMs: Int?,

    @Column("read_timeout_ms")
    val readTimeoutMs: Int?,

    @Column("write_timeout_ms")
    val writeTimeoutMs: Int?,

    @Column("default_headers")
    val defaultHeaders: String?,

    @Column("created_at")
    val createdAt: LocalDateTime?,

    @Column("updated_at")
    val updatedAt: LocalDateTime?
) : Persistable<String> {

    @Transient
    private var isNew: Boolean = false

    constructor(
        id: String,
        baseUrl: String?,
        connectTimeoutMs: Int?,
        readTimeoutMs: Int?,
        writeTimeoutMs: Int?,
        defaultHeaders: String?,
        createdAt: LocalDateTime?,
        updatedAt: LocalDateTime?,
        isNew: Boolean
    ) : this(
        id,
        baseUrl,
        connectTimeoutMs,
        readTimeoutMs,
        writeTimeoutMs,
        defaultHeaders,
        createdAt,
        updatedAt
    ) {
        this.isNew = isNew
    }

    override fun getId(): String = id

    override fun isNew(): Boolean = isNew

    fun markNew(): WebClientConfigEntity {
        this.isNew = true
        return this
    }
}

package com.yosep.server.infrastructure.db.common.entity

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("mydata_org_info")
data class MydataOrgInfoEntity private constructor(
    @Id
    @Column("org_code")
    val orgCode: String,

    @Column("org_type")
    val orgType: String?,

    @Column("org_name")
    val orgName: String?,

    @Column("org_regno")
    val orgRegno: String?,

    @Column("corp_regno")
    val corpRegno: String?,

    @Column("serial_num")
    val serialNum: String?,

    @Column("address")
    val address: String?,

    @Column("domain")
    val domain: String?,

    @Column("relay_org_code")
    val relayOrgCode: String?,

    @Column("industry")
    val industry: String?,

    @Column("auth_type")
    val authType: String?,

    @Column("cert_issuer_dn")
    val certIssuerDn: String?,

    @Column("cert_oid")
    val certOid: String?,

    @Column("insert_time")
    val insertTime: LocalDateTime?,

    @Column("update_time")
    val updateTime: LocalDateTime?
) : Persistable<String> {

    @Transient
    private var isNew: Boolean = false

    // 보조 생성자: isNew 포함
    constructor(
        orgCode: String,
        orgType: String?,
        orgName: String?,
        orgRegno: String?,
        corpRegno: String?,
        serialNum: String?,
        address: String?,
        domain: String?,
        relayOrgCode: String?,
        industry: String?,
        authType: String?,
        certIssuerDn: String?,
        certOid: String?,
        insertTime: LocalDateTime?,
        updateTime: LocalDateTime?,
        isNew: Boolean
    ) : this(
        orgCode,
        orgType,
        orgName,
        orgRegno,
        corpRegno,
        serialNum,
        address,
        domain,
        relayOrgCode,
        industry,
        authType,
        certIssuerDn,
        certOid,
        insertTime,
        updateTime
    ) {
        this.isNew = isNew
    }

    override fun getId(): String = orgCode

    override fun isNew(): Boolean = isNew

    fun markNew(): MydataOrgInfoEntity {
        this.isNew = true
        return this
    }
}

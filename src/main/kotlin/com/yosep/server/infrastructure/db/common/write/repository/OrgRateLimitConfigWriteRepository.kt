package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface OrgRateLimitConfigWriteRepository : CoroutineCrudRepository<OrgRateLimitConfigEntity, String> {
}
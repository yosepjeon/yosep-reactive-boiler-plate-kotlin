package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.OrgInfoEntity
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface OrgInfoWriteRepository : CoroutineCrudRepository<OrgInfoEntity, String> {

}
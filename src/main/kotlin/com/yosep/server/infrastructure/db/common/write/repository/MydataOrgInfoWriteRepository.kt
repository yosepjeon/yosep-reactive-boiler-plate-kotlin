package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.MydataOrgInfoEntity
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface MydataOrgInfoWriteRepository : CoroutineCrudRepository<MydataOrgInfoEntity, String> {

}
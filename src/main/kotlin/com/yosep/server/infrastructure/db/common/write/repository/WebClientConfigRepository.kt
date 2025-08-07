package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.infrastructure.db.common.entity.WebClientConfigEntity
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface WebClientConfigRepository : CoroutineCrudRepository<WebClientConfigEntity, String> {
    
}
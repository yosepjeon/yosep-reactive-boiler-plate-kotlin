package com.yosep.server.common.properties

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "spring.redis")
data class RedisProperties(
    val master: String,
    val slave: String
)
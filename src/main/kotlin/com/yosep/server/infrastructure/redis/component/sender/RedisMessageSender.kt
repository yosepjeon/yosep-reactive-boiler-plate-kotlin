package com.yosep.server.infrastructure.redis.component.sender

import org.redisson.api.RedissonReactiveClient
import org.springframework.stereotype.Component
import kotlinx.coroutines.reactive.awaitSingle

@Component
class RedisMessageSender(
    private val redissonReactiveClient: RedissonReactiveClient
) {
    suspend fun send(topic: String, message: String): Long {
        return redissonReactiveClient.getTopic(topic)
            .publish(message)
            .awaitSingle()
    }
}
package com.yosep.server.common.component.ratelimit

import jakarta.annotation.PostConstruct
import kotlinx.coroutines.reactor.awaitSingle
import org.redisson.api.RScript
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

@Component
class ReactiveSlidingWindowRateLimiter(
    private val redissonReactiveClient: RedissonReactiveClient,
) {

    @Value("classpath:scripts/lua/flow-control/sliding_window_counter.lua")
    private lateinit var luaSlidingWindowResource: Resource

    private lateinit var script: String

    @PostConstruct
    fun loadScript() {
        script = luaSlidingWindowResource.inputStream.readAllBytes().toString(StandardCharsets.UTF_8)
    }

    suspend fun tryAcquireSuspend(key: String, maxQps: Int, windowMs: Int): Boolean {
        val result: Long = redissonReactiveClient.script.eval<Long>(
            RScript.Mode.READ_WRITE,
            script,
            RScript.ReturnType.INTEGER,
            listOf(key),
            *arrayOf(
                windowMs.toString(),
                maxQps.toString()),
        ).awaitSingle()
        return result == 1L
    }
}

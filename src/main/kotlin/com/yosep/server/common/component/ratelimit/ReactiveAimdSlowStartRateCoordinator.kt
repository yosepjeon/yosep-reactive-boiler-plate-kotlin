package com.yosep.server.common.component.ratelimit

import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.redisson.api.RBucketReactive
import org.redisson.api.RScript
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

@Component
class ReactiveAimdSlowStartRateCoordinator(
    private val redissonReactiveClient: RedissonReactiveClient,
    private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
) {

    @field:Value("classpath:scripts/lua/flow-control/aimd_slow_start_success.lua")
    private lateinit var successScriptResource: Resource

    @field:Value("classpath:scripts/lua/flow-control/aimd_slow_start_failure.lua")
    private lateinit var failureScriptResource: Resource

    private lateinit var successScript: String
    private lateinit var failureScript: String

    private companion object {
        private const val MAX_LIMIT: Int = 100_000
        private const val MIN_LIMIT: Int = 30
    }

    @PostConstruct
    fun loadScripts() {
        successScript = successScriptResource.inputStream.readAllBytes().toString(StandardCharsets.UTF_8)
        failureScript = failureScriptResource.inputStream.readAllBytes().toString(StandardCharsets.UTF_8)
    }

    suspend fun getCurrentLimit(org: String): Int {
        val redisKey = "rate:config:$org"
        val bucket: RBucketReactive<String> = redissonReactiveClient.getBucket(redisKey)

        // Try Redis first
        val currentLimitStr: String? = bucket.get().awaitSingleOrNull()
        val currentLimit = currentLimitStr?.toIntOrNull()
        if (currentLimit != null) return currentLimit

        // Fallback to DB default (coroutines repository)
        val orgRateLimitConfigEntity: OrgRateLimitConfigEntity? = orgRateLimitConfigRepository.findById(org)
        val defaultQps = (orgRateLimitConfigEntity?.initialQps ?: 10000)

        // Save back to Redis (ignore completion value)
        bucket.set(defaultQps.toString()).awaitSingleOrNull()
        return defaultQps
    }

    suspend fun reportSuccess(org: String): Int {
        val result: Long = redissonReactiveClient.script.eval<Long>(
            RScript.Mode.READ_WRITE,
            successScript,
            RScript.ReturnType.INTEGER,
            listOf("rate:config:$org"),
            *arrayOf(MAX_LIMIT)
        ).awaitSingle()
        return result.toInt()
    }

    suspend fun reportFailure(org: String): Int {
        val result: Long = redissonReactiveClient.script.eval<Long>(
            RScript.Mode.READ_WRITE,
            failureScript,
            RScript.ReturnType.INTEGER,
            listOf("rate:config:$org"),
            *arrayOf(MIN_LIMIT)
        ).awaitSingle()
        return result.toInt()
    }
}

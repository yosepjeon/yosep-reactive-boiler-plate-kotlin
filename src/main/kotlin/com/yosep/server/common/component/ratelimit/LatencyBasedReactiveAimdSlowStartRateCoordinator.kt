package com.yosep.server.common.component.ratelimit

import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.redisson.api.RBucketReactive
import org.redisson.api.RMapReactive
import org.redisson.api.RScript
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

@Component
class LatencyBasedReactiveAimdSlowStartRateCoordinator(
    private val redissonReactiveClient: RedissonReactiveClient,
    private val orgRateLimitConfigRepository: OrgRateLimitConfigWriteRepository,
) {

    @field:Value("classpath:scripts/lua/flow-control/latency_based_aimd.lua")
    private lateinit var unifiedScriptResource: Resource

    private lateinit var unifiedScript: String

    // Tunables (overridable via application.yml)
    @Value("\${ratelimit.max-limit:100000}")
    private var maxLimit: Int = 100_000

    @Value("\${ratelimit.min-limit:30}")
    private var minLimit: Int = 30

    @Value("\${ratelimit.latency.threshold-ms:500}")
    private var defaultThresholdMs: Long = 500

    @Value("\${ratelimit.failure.n-threshold:3}")
    private var defaultNThreshold: Int = 3

    @Value("\${ratelimit.failure.md:0.5}")
    private var defaultMd: Double = 0.5

    @Value("\${ratelimit.failure.counter-ttl-sec:60}")
    private var defaultCounterTtlSec: Long = 60

    @Value("\${ratelimit.slowstart.n:2}")
    private var defaultN: Int = 2

    @Value("\${ratelimit.slowstart.m:1}")
    private var defaultM: Int = 1

    @PostConstruct
    fun loadScripts() {
        unifiedScriptResource.inputStream.use { unifiedScript = it.readAllBytes()
            .toString(StandardCharsets.UTF_8) }
    }

    suspend fun getCurrentLimit(org: String): Int {
        val redisKey = "rate:config:$org"

        // 1) Try hash: HGET limit_qps
        try {
            val map: RMapReactive<String, String> = redissonReactiveClient.getMap(redisKey)
            val hashLimitStr: String? = map.get("limit_qps").awaitSingleOrNull()
            val hashLimit = hashLimitStr?.toIntOrNull()
            if (hashLimit != null) return hashLimit
        } catch (e: Exception) {
            // ignore and fallback to legacy bucket
        }

        // 2) Fallback legacy string bucket
        val bucket: RBucketReactive<String> = redissonReactiveClient.getBucket(redisKey)
        val bucketStr: String? = try { bucket.get().awaitSingleOrNull() } catch (e: Exception) { null }
        val bucketLimit = bucketStr?.toIntOrNull()
        if (bucketLimit != null) return bucketLimit

        // 3) Initialize default in hash
        val orgRateLimitConfigEntity: OrgRateLimitConfigEntity? = orgRateLimitConfigRepository.findById(org)
        val defaultQps = (orgRateLimitConfigEntity?.initialQps ?: 10_000)

        try {
            val map: RMapReactive<String, String> = redissonReactiveClient.getMap(redisKey)
            map.fastPut("limit_qps", defaultQps.toString()).awaitSingleOrNull()
        } catch (e: Exception) {
            // last resort: legacy string
            bucket.set(defaultQps.toString()).awaitSingleOrNull()
        }
        return defaultQps
    }

    // Success path: use unified script with flag=1
    suspend fun reportSuccess(
        org: String,
        n: Int = defaultN,
        m: Int = defaultM,
        latencyMs: Long = 0,
        thresholdMs: Long = defaultThresholdMs,
    ): Int {
        val limitKey = "rate:config:$org"
        val counterKey = "rate:viol:$org"
        val result: Long = redissonReactiveClient.script.eval<Long>(
            RScript.Mode.READ_WRITE,
            unifiedScript,
            RScript.ReturnType.INTEGER,
            listOf(limitKey, counterKey),
            *arrayOf<Any>(
                1,              // ARGV[1] success flag
                maxLimit,       // ARGV[2] max_limit
                minLimit,       // ARGV[3] min_limit
                n,              // ARGV[4] n (multiply factor)
                m,              // ARGV[5] m (additive increment)
                latencyMs,      // ARGV[6] latency_ms
                thresholdMs,    // ARGV[7] latency_threshold_ms
                defaultMd,      // ARGV[8] md
                defaultNThreshold, // ARGV[9] n_threshold
                defaultCounterTtlSec // ARGV[10] counter_ttl_sec
            )
        ).awaitSingle()
        return result.toInt()
    }

    // Failure path: use unified script with flag=0
    suspend fun reportFailure(
        org: String,
        latencyMs: Long = defaultThresholdMs,
        thresholdMs: Long = defaultThresholdMs,
        nThreshold: Int = defaultNThreshold,
        md: Double = defaultMd,
        counterTtlSec: Long = defaultCounterTtlSec,
        n: Int = defaultN,
        m: Int = defaultM,
    ): Int {
        val limitKey = "rate:config:$org"
        val counterKey = "rate:viol:$org"
        val result: Long = redissonReactiveClient.script.eval<Long>(
            RScript.Mode.READ_WRITE,
            unifiedScript,
            RScript.ReturnType.INTEGER,
            listOf(limitKey, counterKey),
            *arrayOf<Any>(
                0,              // ARGV[1] success flag
                maxLimit,       // ARGV[2] max_limit
                minLimit,       // ARGV[3] min_limit
                n,              // ARGV[4] n (not used in failure decision, but required by script)
                m,              // ARGV[5] m (not used in failure decision)
                latencyMs,      // ARGV[6] latency_ms
                thresholdMs,    // ARGV[7] latency_threshold_ms
                md,             // ARGV[8] md
                nThreshold,     // ARGV[9] n_threshold
                counterTtlSec   // ARGV[10] counter_ttl_sec
            )
        ).awaitSingle()
        return result.toInt()
    }
}

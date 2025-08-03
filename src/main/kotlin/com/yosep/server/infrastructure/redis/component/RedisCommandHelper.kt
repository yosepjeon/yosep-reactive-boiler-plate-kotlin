package com.yosep.server.infrastructure.redis.component

import kotlinx.coroutines.reactor.awaitSingleOrNull
import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import org.redisson.api.*
import org.springframework.stereotype.Component
import org.springframework.util.ObjectUtils
import java.io.Serializable
import java.util.concurrent.TimeUnit

@Component
@Slf4j
@RequiredArgsConstructor
class RedisCommandHelper(
    private val redissonReactiveClient: RedissonReactiveClient
) {

    private fun getRedisBucket(key: String): RBucketReactive<Any> {
        return this.redissonReactiveClient.getBucket(key)
    }

    private fun getZset(key: String): RScoredSortedSetReactive<String> {
        return this.redissonReactiveClient.getScoredSortedSet(key)
    }

    suspend fun get(key: String): Any? {
        val temp = getRedisBucket(key)
        return temp.get().awaitSingleOrNull()
    }

    suspend fun set(key: String, value: Serializable, ttl: Long) {
        val temp = getRedisBucket(key)
        temp.set(value, ttl, TimeUnit.SECONDS).awaitSingleOrNull()
    }

    suspend fun exists(key: String): Boolean? {
        val temp = getRedisBucket(key)
        return temp.isExists().awaitSingleOrNull()
    }

    suspend fun type(key: String): String? {
        val type = redissonReactiveClient.getKeys().getType(key).awaitSingleOrNull()
        return type?.javaClass?.name
    }

    suspend fun zAddByLock(key: String, score: Long, value: String): Boolean? {
        if (ObjectUtils.isEmpty(key)) {
            return null
        }

        val lockName = "$key:lock"
        val redissonClientLock = redissonReactiveClient.getLock(lockName)

        val isLocked = redissonClientLock.tryLock(3, 3, TimeUnit.SECONDS).awaitSingleOrNull()
        return if (isLocked == true) {
            try {
                val zSet = getZset(key)
                zSet.add(score.toDouble(), value).awaitSingleOrNull()
            } finally {
                val locked = redissonClientLock.isLocked.awaitSingleOrNull()
                if (locked == true) {
                    redissonClientLock.unlock().awaitSingleOrNull()
                }
            }
        } else {
            false
        }
    }
}
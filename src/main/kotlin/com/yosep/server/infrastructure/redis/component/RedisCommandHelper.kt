package com.yosep.server.infrastructure.redis.component

import kotlinx.coroutines.reactor.awaitSingleOrNull
import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import org.redisson.api.*
import org.springframework.stereotype.Component
import org.springframework.util.ObjectUtils
import java.io.Serializable
import java.time.Duration
import java.util.concurrent.TimeUnit

@Component
@Slf4j
@RequiredArgsConstructor
class RedisCommandHelper(
    private val redissonReactiveClient: RedissonReactiveClient
) {

    private fun getRedisBucket(key: String): RBucketReactive<Any> {
        return redissonReactiveClient.getBucket(key)
    }

    private fun getMap(key: String): RMapReactive<String, Any> {
        return redissonReactiveClient.getMap(key)
    }

    private fun getSet(key: String): RSetReactive<Any> {
        return redissonReactiveClient.getSet(key)
    }

    private fun getZset(key: String): RScoredSortedSetReactive<String> {
        return redissonReactiveClient.getScoredSortedSet(key)
    }

    suspend fun get(key: String): Any? {
        return getRedisBucket(key).get().awaitSingleOrNull()
    }

    suspend fun getOrDefault(key: String, defaultValue: Any): Any? {
        return getRedisBucket(key).get().awaitSingleOrNull() ?: defaultValue
    }

    suspend fun set(key: String, value: Serializable, ttl: Long? = null) {
        val bucket = getRedisBucket(key)
        if (ttl != null) {
            bucket.set(value, ttl, TimeUnit.SECONDS).awaitSingleOrNull()
        } else {
            bucket.set(value).awaitSingleOrNull()
        }
    }

    suspend fun setIfAbsent(key: String, value: String, seconds: Long): Boolean {
        return getRedisBucket(key).setIfAbsent(value, Duration.ofSeconds(seconds)).awaitSingleOrNull() ?: false
    }

    suspend fun exists(key: String): Boolean {
        return getRedisBucket(key).isExists.awaitSingleOrNull() ?: false
    }

    suspend fun delete(key: String) {
        redissonReactiveClient.getKeys().delete(key).awaitSingleOrNull()
    }

    suspend fun expire(key: String, seconds: Long): Boolean {
        return getRedisBucket(key).expire(Duration.ofSeconds(seconds)).awaitSingleOrNull() ?: false
    }

    suspend fun type(key: String): String? {
        return redissonReactiveClient.getKeys().getType(key).awaitSingleOrNull()?.javaClass?.name
    }

    suspend fun getTtl(key: String): Long? {
        return getRedisBucket(key).remainTimeToLive().awaitSingleOrNull()
    }

    suspend fun getSet(key: String, value: Serializable): Any? {
        return getRedisBucket(key).getAndSet(value).awaitSingleOrNull()
    }

    suspend fun hset(key: String, field: String, value: Any, seconds: Long? = null) {
        getMap(key).put(field, value).awaitSingleOrNull()
        if (seconds != null) {
            expire(key, seconds)
        }
    }

    suspend fun hget(key: String, field: String): Any? {
        return getMap(key).get(field).awaitSingleOrNull()
    }

    suspend fun hdel(key: String, field: String) {
        getMap(key).remove(field).awaitSingleOrNull()
    }

    suspend fun hmset(key: String, map: Map<String, Any>, seconds: Long? = null) {
        getMap(key).putAll(map).awaitSingleOrNull()
        if (seconds != null) {
            expire(key, seconds)
        }
    }

    suspend fun hmget(key: String, fields: Set<String>? = null): Map<String, Any> {
        val map = getMap(key)
        return if (fields != null) {
            map.getAll(fields).awaitSingleOrNull() ?: emptyMap()
        } else {
            map.readAllMap().awaitSingleOrNull() ?: emptyMap()
        }
    }

    suspend fun hexists(key: String, field: String? = null): Boolean {
        val map = getMap(key)
        return if (field != null) {
            map.containsKey(field).awaitSingleOrNull() ?: false
        } else {
            val size = map.size().awaitSingleOrNull() ?: 0
            size > 0
        }
    }

    suspend fun sadd(key: String, value: Serializable): Boolean {
        return getSet(key).add(value).awaitSingleOrNull() ?: false
    }

    suspend fun sall(key: String): Set<Any> {
        return getSet(key).readAll().awaitSingleOrNull() ?: emptySet()
    }

    suspend fun sdel(key: String, value: Serializable): Boolean {
        return getSet(key).remove(value).awaitSingleOrNull() ?: false
    }

    suspend fun incr(key: String, seconds: Long? = null): Long {
        val atomic = redissonReactiveClient.getAtomicLong(key)
        val result = atomic.incrementAndGet().awaitSingleOrNull() ?: 0L
        if (seconds != null) {
            atomic.expire(Duration.ofSeconds(seconds)).awaitSingleOrNull()
        }
        return result
    }

    suspend fun zAddByLock(key: String, score: Long, value: String): Boolean? {
        if (ObjectUtils.isEmpty(key)) return null
        val lockName = "$key:lock"
        val lock = redissonReactiveClient.getLock(lockName)
        val locked = lock.tryLock(3, 3, TimeUnit.SECONDS).awaitSingleOrNull() ?: false
        return if (locked) {
            try {
                getZset(key).add(score.toDouble(), value).awaitSingleOrNull()
            } finally {
                if (lock.isLocked.awaitSingleOrNull() == true) {
                    lock.unlock().awaitSingleOrNull()
                }
            }
        } else false
    }

    fun getLock(lockName: String): RLockReactive {
        return redissonReactiveClient.getLock(lockName)
    }
}

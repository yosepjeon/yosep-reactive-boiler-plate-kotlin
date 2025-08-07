package com.yosep.server.infrastructure.redis.component

import com.yosep.server.common.AbstractIntegrationContainerBase
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class RedisCommandHelperTest(
    @Autowired private val redisCommandHelper: RedisCommandHelper
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis set and get test with coroutine`() = runTest {
        val key = "redis-test"
        val value = "test"

        redisCommandHelper.set(key, value, 5)

        val result = redisCommandHelper.get(key)

        assertEquals(value, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis getOrDefault test with existing key`() = runTest {
        val key = "redis-get-or-default-test"
        val value = "existing-value"
        val defaultValue = "default-value"

        redisCommandHelper.set(key, value, 5)

        val result = redisCommandHelper.getOrDefault(key, defaultValue)

        assertEquals(value, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis getOrDefault test with non-existing key`() = runTest {
        val key = "redis-non-existing-key"
        val defaultValue = "default-value"

        val result = redisCommandHelper.getOrDefault(key, defaultValue)

        assertEquals(defaultValue, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis set without ttl test`() = runTest {
        val key = "redis-set-no-ttl-test"
        val value = "test-value"

        redisCommandHelper.set(key, value)

        val result = redisCommandHelper.get(key)

        assertEquals(value, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis setIfAbsent test with new key`() = runTest {
        val key = "redis-set-if-absent-new-test"
        val value = "test-value"

        val result = redisCommandHelper.setIfAbsent(key, value, 5)

        assertTrue(result)
        assertEquals(value, redisCommandHelper.get(key))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis setIfAbsent test with existing key`() = runTest {
        val key = "redis-set-if-absent-existing-test"
        val value1 = "first-value"
        val value2 = "second-value"

        redisCommandHelper.set(key, value1, 5)
        val result = redisCommandHelper.setIfAbsent(key, value2, 5)

        assertFalse(result)
        assertEquals(value1, redisCommandHelper.get(key))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis exists test with existing key`() = runTest {
        val key = "redis-exists-test"
        val value = "test-value"

        redisCommandHelper.set(key, value, 5)

        val result = redisCommandHelper.exists(key)

        assertTrue(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis exists test with non-existing key`() = runTest {
        val key = "redis-non-existing-key"

        val result = redisCommandHelper.exists(key)

        assertFalse(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis delete test`() = runTest {
        val key = "redis-delete-test"
        val value = "test-value"

        redisCommandHelper.set(key, value, 5)
        assertTrue(redisCommandHelper.exists(key))

        redisCommandHelper.delete(key)

        assertFalse(redisCommandHelper.exists(key))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis expire test`() = runTest {
        val key = "redis-expire-test"
        val value = "test-value"

        redisCommandHelper.set(key, value)
        val result = redisCommandHelper.expire(key, 10)

        assertTrue(result)
        val ttl = redisCommandHelper.getTtl(key)
        assertNotNull(ttl)
        assertTrue(ttl!! > 0)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis type test`() = runTest {
        val key = "redis-type-test"
        val value = "test-value"

        redisCommandHelper.set(key, value, 5)

        val result = redisCommandHelper.type(key)

        assertNotNull(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis getTtl test`() = runTest {
        val key = "redis-get-ttl-test"
        val value = "test-value"
        val ttlSeconds = 10L

        redisCommandHelper.set(key, value, ttlSeconds)

        val result = redisCommandHelper.getTtl(key)

        assertNotNull(result)
        assertTrue(result!! > 0)
        assertTrue(result <= ttlSeconds * 1000) // TTL is in milliseconds
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis getSet test`() = runTest {
        val key = "redis-get-set-test"
        val oldValue = "old-value"
        val newValue = "new-value"

        redisCommandHelper.set(key, oldValue, 5)

        val result = redisCommandHelper.getSet(key, newValue)

        assertEquals(oldValue, result)
        assertEquals(newValue, redisCommandHelper.get(key))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hset and hget test`() = runTest {
        val key = "redis-hash-test"
        val field = "test-field"
        val value = "test-value"

        redisCommandHelper.hset(key, field, value, 5)

        val result = redisCommandHelper.hget(key, field)

        assertEquals(value, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hset without ttl test`() = runTest {
        val key = "redis-hash-no-ttl-test"
        val field = "test-field"
        val value = "test-value"

        redisCommandHelper.hset(key, field, value)

        val result = redisCommandHelper.hget(key, field)

        assertEquals(value, result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hdel test`() = runTest {
        val key = "redis-hash-del-test"
        val field = "test-field"
        val value = "test-value"

        redisCommandHelper.hset(key, field, value, 5)
        assertEquals(value, redisCommandHelper.hget(key, field))

        redisCommandHelper.hdel(key, field)

        assertNull(redisCommandHelper.hget(key, field))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hmset and hmget test`() = runTest {
        val key = "redis-hash-multi-test"
        val map = mapOf(
            "field1" to "value1",
            "field2" to "value2",
            "field3" to "value3"
        )

        redisCommandHelper.hmset(key, map, 5)

        val result = redisCommandHelper.hmget(key)

        assertEquals(map.size, result.size)
        assertEquals("value1", result["field1"])
        assertEquals("value2", result["field2"])
        assertEquals("value3", result["field3"])
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hmset without ttl test`() = runTest {
        val key = "redis-hash-multi-no-ttl-test"
        val map = mapOf(
            "field1" to "value1",
            "field2" to "value2"
        )

        redisCommandHelper.hmset(key, map)

        val result = redisCommandHelper.hmget(key)

        assertEquals(map.size, result.size)
        assertEquals("value1", result["field1"])
        assertEquals("value2", result["field2"])
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hmget with specific fields test`() = runTest {
        val key = "redis-hash-multi-specific-test"
        val map = mapOf(
            "field1" to "value1",
            "field2" to "value2",
            "field3" to "value3"
        )

        redisCommandHelper.hmset(key, map, 5)

        val fields = setOf("field1", "field3")
        val result = redisCommandHelper.hmget(key, fields)

        assertEquals(2, result.size)
        assertEquals("value1", result["field1"])
        assertEquals("value3", result["field3"])
        assertNull(result["field2"])
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hexists test with field`() = runTest {
        val key = "redis-hash-exists-test"
        val field = "test-field"
        val value = "test-value"

        redisCommandHelper.hset(key, field, value, 5)

        val result = redisCommandHelper.hexists(key, field)

        assertTrue(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hexists test without field`() = runTest {
        val key = "redis-hash-exists-no-field-test"
        val field = "test-field"
        val value = "test-value"

        redisCommandHelper.hset(key, field, value, 5)

        val result = redisCommandHelper.hexists(key)

        assertTrue(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis hexists test with non-existing field`() = runTest {
        val key = "redis-hash-not-exists-test"
        val field = "non-existing-field"

        val result = redisCommandHelper.hexists(key, field)

        assertFalse(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis sadd and sall test`() = runTest {
        val key = "redis-set-test"
        val value1 = "value1"
        val value2 = "value2"
        val value3 = "value3"

        val result1 = redisCommandHelper.sadd(key, value1)
        val result2 = redisCommandHelper.sadd(key, value2)
        val result3 = redisCommandHelper.sadd(key, value3)

        assertTrue(result1)
        assertTrue(result2)
        assertTrue(result3)

        val allValues = redisCommandHelper.sall(key)

        assertEquals(3, allValues.size)
        assertTrue(allValues.contains(value1))
        assertTrue(allValues.contains(value2))
        assertTrue(allValues.contains(value3))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis sadd duplicate value test`() = runTest {
        val key = "redis-set-duplicate-test"
        val value = "duplicate-value"

        val result1 = redisCommandHelper.sadd(key, value)
        val result2 = redisCommandHelper.sadd(key, value)

        assertTrue(result1)
        assertFalse(result2)

        val allValues = redisCommandHelper.sall(key)
        assertEquals(1, allValues.size)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis sdel test`() = runTest {
        val key = "redis-set-del-test"
        val value1 = "value1"
        val value2 = "value2"

        redisCommandHelper.sadd(key, value1)
        redisCommandHelper.sadd(key, value2)

        val result = redisCommandHelper.sdel(key, value1)

        assertTrue(result)

        val allValues = redisCommandHelper.sall(key)
        assertEquals(1, allValues.size)
        assertFalse(allValues.contains(value1))
        assertTrue(allValues.contains(value2))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis incr test`() = runTest {
        val key = "redis-incr-test"

        val result1 = redisCommandHelper.incr(key, 5)
        val result2 = redisCommandHelper.incr(key, 5)
        val result3 = redisCommandHelper.incr(key, 5)

        assertEquals(1L, result1)
        assertEquals(2L, result2)
        assertEquals(3L, result3)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis incr without ttl test`() = runTest {
        val key = "redis-incr-no-ttl-test"

        val result1 = redisCommandHelper.incr(key)
        val result2 = redisCommandHelper.incr(key)

        assertEquals(1L, result1)
        assertEquals(2L, result2)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis zAddByLock test`() = runTest {
        val key = "redis-zset-test"
        val score = 100L
        val value = "test-member"

        val result = redisCommandHelper.zAddByLock(key, score, value)

        assertTrue(result ?: false)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis zAddByLock with empty key test`() = runTest {
        val key = ""
        val score = 100L
        val value = "test-member"

        val result = redisCommandHelper.zAddByLock(key, score, value)

        assertNull(result)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis getLock test`() = runTest {
        val lockName = "test-lock"

        val lock = redisCommandHelper.getLock(lockName)

        assertNotNull(lock)
    }
}
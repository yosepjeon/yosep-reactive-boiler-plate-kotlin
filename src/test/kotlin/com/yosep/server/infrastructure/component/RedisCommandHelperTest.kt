package com.yosep.server.infrastructure.component

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("local")
class RedisCommandHelperTest(
    @Autowired private val redisCommandHelper: RedisCommandHelper
) {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `redis set and get test with coroutine`() = runTest {
        val key = "redis-test"
        val value = "test"

        redisCommandHelper.set(key, value, 5)

        val result = redisCommandHelper.get(key)

        assertEquals(value, result)
    }
}
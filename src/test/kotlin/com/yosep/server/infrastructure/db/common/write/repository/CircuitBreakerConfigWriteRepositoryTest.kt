package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.CircuitBreakerConfigEntity
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CircuitBreakerConfigWriteRepositoryTest(
    @Autowired private val circuitBreakerConfigWriteRepository: CircuitBreakerConfigWriteRepository
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `save and findById test`() = runTest {
        val entity = CircuitBreakerConfigEntity(
            "test-breaker-1",
            50,
            80,
            1000,
            60000,
            10,
            100,
            10,
            "COUNT_BASED",
            "500,502,503",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        val savedEntity = circuitBreakerConfigWriteRepository.save(entity)
        assertNotNull(savedEntity)
        assertEquals("test-breaker-1", savedEntity.breakerName)

        val foundEntity = circuitBreakerConfigWriteRepository.findById("test-breaker-1")
        assertNotNull(foundEntity)
        assertEquals("test-breaker-1", foundEntity?.breakerName)
        assertEquals(50, foundEntity?.failureRateThreshold)
        assertEquals(80, foundEntity?.slowCallRateThreshold)
        assertEquals(1000, foundEntity?.slowCallDurationThresholdMs)
        assertEquals("COUNT_BASED", foundEntity?.slidingWindowType)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findByBreakerName test`() = runTest {
        val entity = CircuitBreakerConfigEntity(
            "custom-breaker-test",
            60,
            90,
            2000,
            120000,
            5,
            50,
            20,
            "TIME_BASED",
            "400,404,500",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        circuitBreakerConfigWriteRepository.save(entity)

        val foundEntity = circuitBreakerConfigWriteRepository.findByBreakerName("custom-breaker-test")
        assertNotNull(foundEntity)
        assertEquals("custom-breaker-test", foundEntity?.breakerName)
        assertEquals(60, foundEntity?.failureRateThreshold)
        assertEquals("TIME_BASED", foundEntity?.slidingWindowType)
        assertEquals("400,404,500", foundEntity?.recordFailureStatusCodes)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findByBreakerName with non-existing breaker test`() = runTest {
        val foundEntity = circuitBreakerConfigWriteRepository.findByBreakerName("non-existing-breaker")
        assertNull(foundEntity)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findAll test`() = runTest {
        val entity1 = CircuitBreakerConfigEntity(
            "test-breaker-2",
            40,
            70,
            800,
            30000,
            15,
            200,
            5,
            "COUNT_BASED",
            "500,503",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        val entity2 = CircuitBreakerConfigEntity(
            "test-breaker-3",
            30,
            60,
            1500,
            90000,
            8,
            150,
            15,
            "TIME_BASED",
            "502,504",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        circuitBreakerConfigWriteRepository.save(entity1)
        circuitBreakerConfigWriteRepository.save(entity2)

        val allEntities = circuitBreakerConfigWriteRepository.findAll().toList()
        assertTrue(allEntities.size >= 2)
        
        val savedBreakerNames = allEntities.map { it.breakerName }
        assertTrue(savedBreakerNames.contains("test-breaker-2"))
        assertTrue(savedBreakerNames.contains("test-breaker-3"))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `delete test`() = runTest {
        val entity = CircuitBreakerConfigEntity(
            "test-breaker-delete",
            45,
            75,
            1200,
            45000,
            12,
            80,
            8,
            "COUNT_BASED",
            "500",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        circuitBreakerConfigWriteRepository.save(entity)
        
        val foundBeforeDelete = circuitBreakerConfigWriteRepository.findById("test-breaker-delete")
        assertNotNull(foundBeforeDelete)

        circuitBreakerConfigWriteRepository.deleteById("test-breaker-delete")

        val foundAfterDelete = circuitBreakerConfigWriteRepository.findById("test-breaker-delete")
        assertNull(foundAfterDelete)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `existsById test`() = runTest {
        val entity = CircuitBreakerConfigEntity(
            "test-breaker-exists",
            35,
            65,
            900,
            75000,
            6,
            120,
            12,
            "TIME_BASED",
            "400,500,502",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        val existsBeforeSave = circuitBreakerConfigWriteRepository.existsById("test-breaker-exists")
        assertFalse(existsBeforeSave)

        circuitBreakerConfigWriteRepository.save(entity)

        val existsAfterSave = circuitBreakerConfigWriteRepository.existsById("test-breaker-exists")
        assertTrue(existsAfterSave)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `update test`() = runTest {
        val entity = CircuitBreakerConfigEntity(
            "test-breaker-update",
            50,
            80,
            1000,
            60000,
            10,
            100,
            10,
            "COUNT_BASED",
            "500,502",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        ).markNew()

        circuitBreakerConfigWriteRepository.save(entity)

        val updatedEntity = CircuitBreakerConfigEntity(
            "test-breaker-update",
            70,
            90,
            2000,
            120000,
            5,
            50,
            20,
            "TIME_BASED",
            "400,500,502,503",
            entity.createdAt,
            LocalDateTime.now(),
            false
        )

        val savedUpdatedEntity = circuitBreakerConfigWriteRepository.save(updatedEntity)
        assertEquals(70, savedUpdatedEntity.failureRateThreshold)
        assertEquals(90, savedUpdatedEntity.slowCallRateThreshold)
        assertEquals(2000, savedUpdatedEntity.slowCallDurationThresholdMs)
        assertEquals("TIME_BASED", savedUpdatedEntity.slidingWindowType)
        assertEquals("400,500,502,503", savedUpdatedEntity.recordFailureStatusCodes)
    }
}
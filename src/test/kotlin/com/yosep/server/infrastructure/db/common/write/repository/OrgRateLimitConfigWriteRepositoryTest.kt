package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.OrgRateLimitConfigEntity
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
class OrgRateLimitConfigWriteRepositoryTest(
    @Autowired private val orgRateLimitConfigWriteRepository: OrgRateLimitConfigWriteRepository
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `save and findById test`() = runTest {
        val entity = OrgRateLimitConfigEntity(
            "test-org-1",
            100,
            1000,
            10,
            500,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val savedEntity = orgRateLimitConfigWriteRepository.save(entity)
        assertNotNull(savedEntity)
        assertEquals("test-org-1", savedEntity.id)

        val foundEntity = orgRateLimitConfigWriteRepository.findById("test-org-1")
        assertNotNull(foundEntity)
        assertEquals("test-org-1", foundEntity?.id)
        assertEquals(100, foundEntity?.initialQps)
        assertEquals(1000, foundEntity?.maxQps)
        assertEquals(10, foundEntity?.minQps)
        assertEquals(500, foundEntity?.latencyThreshold)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findAll test`() = runTest {
        val entity1 = OrgRateLimitConfigEntity(
            "test-org-2",
            200,
            2000,
            20,
            600,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val entity2 = OrgRateLimitConfigEntity(
            "test-org-3",
            300,
            3000,
            30,
            700,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgRateLimitConfigWriteRepository.save(entity1)
        orgRateLimitConfigWriteRepository.save(entity2)

        val allEntities = orgRateLimitConfigWriteRepository.findAll().toList()
        assertTrue(allEntities.size >= 2)
        
        val savedIds = allEntities.map { it.id }
        assertTrue(savedIds.contains("test-org-2"))
        assertTrue(savedIds.contains("test-org-3"))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `delete test`() = runTest {
        val entity = OrgRateLimitConfigEntity(
            "test-org-delete",
            150,
            1500,
            15,
            550,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgRateLimitConfigWriteRepository.save(entity)
        
        val foundBeforeDelete = orgRateLimitConfigWriteRepository.findById("test-org-delete")
        assertNotNull(foundBeforeDelete)

        orgRateLimitConfigWriteRepository.deleteById("test-org-delete")

        val foundAfterDelete = orgRateLimitConfigWriteRepository.findById("test-org-delete")
        assertNull(foundAfterDelete)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `existsById test`() = runTest {
        val entity = OrgRateLimitConfigEntity(
            "test-org-exists",
            50,
            500,
            5,
            400,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val existsBeforeSave = orgRateLimitConfigWriteRepository.existsById("test-org-exists")
        assertFalse(existsBeforeSave)

        orgRateLimitConfigWriteRepository.save(entity)

        val existsAfterSave = orgRateLimitConfigWriteRepository.existsById("test-org-exists")
        assertTrue(existsAfterSave)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `update test`() = runTest {
        val entity = OrgRateLimitConfigEntity(
            "test-org-update",
            100,
            1000,
            10,
            500,
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgRateLimitConfigWriteRepository.save(entity)

        val updatedEntity = OrgRateLimitConfigEntity(
            "test-org-update",
            200,
            2000,
            20,
            600,
            entity.createdAt,
            LocalDateTime.now(),
            false
        )

        val savedUpdatedEntity = orgRateLimitConfigWriteRepository.save(updatedEntity)
        assertEquals(200, savedUpdatedEntity.initialQps)
        assertEquals(2000, savedUpdatedEntity.maxQps)
        assertEquals(20, savedUpdatedEntity.minQps)
        assertEquals(600, savedUpdatedEntity.latencyThreshold)
    }
}
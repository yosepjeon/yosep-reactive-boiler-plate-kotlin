package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.WebClientConfigEntity
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
class WebClientConfigRepositoryTest(
    @Autowired private val webClientConfigRepository: WebClientConfigRepository
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `save and findById test`() = runTest {
        val entity = WebClientConfigEntity(
            "test-org-1",
            "https://api.test.com",
            5000,
            10000,
            10000,
            "{\"Content-Type\":\"application/json\"}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val savedEntity = webClientConfigRepository.save(entity)
        assertNotNull(savedEntity)
        assertEquals("test-org-1", savedEntity.id)

        val foundEntity = webClientConfigRepository.findById("test-org-1")
        assertNotNull(foundEntity)
        assertEquals("test-org-1", foundEntity?.id)
        assertEquals("https://api.test.com", foundEntity?.baseUrl)
        assertEquals(5000, foundEntity?.connectTimeoutMs)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findAll test`() = runTest {
        val entity1 = WebClientConfigEntity(
            "test-org-2",
            "https://api.test2.com",
            3000,
            8000,
            8000,
            "{}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val entity2 = WebClientConfigEntity(
            "test-org-3",
            "https://api.test3.com",
            4000,
            9000,
            9000,
            "{}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        webClientConfigRepository.save(entity1)
        webClientConfigRepository.save(entity2)

        val allEntities = webClientConfigRepository.findAll().toList()
        assertTrue(allEntities.size >= 2)
        
        val savedIds = allEntities.map { it.id }
        assertTrue(savedIds.contains("test-org-2"))
        assertTrue(savedIds.contains("test-org-3"))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `delete test`() = runTest {
        val entity = WebClientConfigEntity(
            "test-org-delete",
            "https://api.delete.com",
            2000,
            5000,
            5000,
            "{}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        webClientConfigRepository.save(entity)
        
        val foundBeforeDelete = webClientConfigRepository.findById("test-org-delete")
        assertNotNull(foundBeforeDelete)

        webClientConfigRepository.deleteById("test-org-delete")

        val foundAfterDelete = webClientConfigRepository.findById("test-org-delete")
        assertNull(foundAfterDelete)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `existsById test`() = runTest {
        val entity = WebClientConfigEntity(
            "test-org-exists",
            "https://api.exists.com",
            1000,
            3000,
            3000,
            "{}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val existsBeforeSave = webClientConfigRepository.existsById("test-org-exists")
        assertFalse(existsBeforeSave)

        webClientConfigRepository.save(entity)

        val existsAfterSave = webClientConfigRepository.existsById("test-org-exists")
        assertTrue(existsAfterSave)
    }
}
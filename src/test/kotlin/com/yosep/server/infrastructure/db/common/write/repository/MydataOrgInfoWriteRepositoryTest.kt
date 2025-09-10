package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.OrgInfoEntity
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
class MydataOrgInfoWriteRepositoryTest(
    @Autowired private val orgInfoWriteRepository: OrgInfoWriteRepository
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `save and findById test`() = runTest {
        val uniqueOrgCode = "T${System.currentTimeMillis().toString().takeLast(9)}"
        val entity = OrgInfoEntity(
            uniqueOrgCode,
            "01",
            "Test Bank",
            "123-45-67890",
            "1234567890123",
            "SN001",
            "Seoul, Korea",
            "testbank.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.4.5",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val savedEntity = orgInfoWriteRepository.save(entity)
        assertNotNull(savedEntity)
        assertEquals(uniqueOrgCode, savedEntity.orgCode)

        val foundEntity = orgInfoWriteRepository.findById(uniqueOrgCode)
        assertNotNull(foundEntity)
        assertEquals(uniqueOrgCode, foundEntity?.orgCode)
        assertEquals("01", foundEntity?.orgType)
        assertEquals("Test Bank", foundEntity?.orgName)
        assertEquals("123-45-67890", foundEntity?.orgRegno)
        assertEquals("testbank.com", foundEntity?.domain)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findAll test`() = runTest {
        val uniqueOrgCode1 = "T${System.currentTimeMillis().toString().takeLast(8)}1"
        val uniqueOrgCode2 = "T${System.currentTimeMillis().toString().takeLast(8)}2"
        
        val entity1 = OrgInfoEntity(
            uniqueOrgCode1,
            "01",
            "Test Card Company",
            "234-56-78901",
            "2345678901234",
            "SN002",
            "Busan, Korea",
            "testcard.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            "CN=TestCardCA",
            "1.2.3.4.6",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val entity2 = OrgInfoEntity(
            uniqueOrgCode2,
            "01",
            "Test Insurance",
            "345-67-89012",
            "3456789012345",
            "SN003",
            "Daegu, Korea",
            "testinsurance.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.4.7",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgInfoWriteRepository.save(entity1)
        orgInfoWriteRepository.save(entity2)

        val allEntities = orgInfoWriteRepository.findAll().toList()
        assertTrue(allEntities.size >= 2)

        val savedOrgCodes = allEntities.map { it.orgCode }
        assertTrue(savedOrgCodes.contains(uniqueOrgCode1))
        assertTrue(savedOrgCodes.contains(uniqueOrgCode2))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `delete test`() = runTest {
        val entity = OrgInfoEntity(
            "ORG_DELETE",
            "01",
            "Test Securities",
            "456-78-90123",
            "4567890123456",
            "SN_DELETE",
            "Incheon, Korea",
            "testsecurities.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.4.8",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgInfoWriteRepository.save(entity)

        val foundBeforeDelete = orgInfoWriteRepository.findById("ORG_DELETE")
        assertNotNull(foundBeforeDelete)

        orgInfoWriteRepository.deleteById("ORG_DELETE")

        val foundAfterDelete = orgInfoWriteRepository.findById("ORG_DELETE")
        assertNull(foundAfterDelete)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `existsById test`() = runTest {
        val entity = OrgInfoEntity(
            "ORG_EXISTS",
            "01",
            "Test Capital",
            "567-89-01234",
            "5678901234567",
            "SN_EXISTS",
            "Gwangju, Korea",
            "testcapital.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.4.9",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        val existsBeforeSave = orgInfoWriteRepository.existsById("ORG_EXISTS")
        assertFalse(existsBeforeSave)

        orgInfoWriteRepository.save(entity)

        val existsAfterSave = orgInfoWriteRepository.existsById("ORG_EXISTS")
        assertTrue(existsAfterSave)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `update test`() = runTest {
        val entity = OrgInfoEntity(
            "ORG_UPDATE",
            "01",
            "Original Bank",
            "678-90-12345",
            "6789012345678",
            "SN_UPDATE",
            "Original Address",
            "originalbank.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.5.0",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )

        orgInfoWriteRepository.save(entity)

        val updatedEntity = OrgInfoEntity(
            "ORG_UPDATE",
            "01",
            "Updated Bank",
            "678-90-12345",
            "6789012345678",
            "SN_UPDATE",
            "Updated Address",
            "updatedbank.com",
            "ZWAAEA0000",
            "FINANCE",
            "01",
            null,
            "1.2.3.5.1",
            entity.insertTime,
            LocalDateTime.now(),
            false
        )

        val savedUpdatedEntity = orgInfoWriteRepository.save(updatedEntity)
        assertEquals("Updated Bank", savedUpdatedEntity.orgName)
        assertEquals("Updated Address", savedUpdatedEntity.address)
        assertEquals("updatedbank.com", savedUpdatedEntity.domain)
        assertNull(savedUpdatedEntity.certIssuerDn)
    }
}
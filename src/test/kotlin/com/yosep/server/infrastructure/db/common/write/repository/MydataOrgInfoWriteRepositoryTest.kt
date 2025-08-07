package com.yosep.server.infrastructure.db.common.write.repository

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.MydataOrgInfoEntity
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test")
class MydataOrgInfoWriteRepositoryTest(
    @Autowired private val mydataOrgInfoWriteRepository: MydataOrgInfoWriteRepository
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `save and findById test`() = runTest {
        val entity = MydataOrgInfoEntity(
            "ORG001",
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

        val savedEntity = mydataOrgInfoWriteRepository.save(entity)
        assertNotNull(savedEntity)
        assertEquals("ORG001", savedEntity.orgCode)

        val foundEntity = mydataOrgInfoWriteRepository.findById("ORG001")
        assertNotNull(foundEntity)
        assertEquals("ORG001", foundEntity?.orgCode)
        assertEquals("01", foundEntity?.orgType)
        assertEquals("Test Bank", foundEntity?.orgName)
        assertEquals("123-45-67890", foundEntity?.orgRegno)
        assertEquals("testbank.com", foundEntity?.domain)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `findAll test`() = runTest {
        val entity1 = MydataOrgInfoEntity(
            "ORG002",
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

        val entity2 = MydataOrgInfoEntity(
            "ORG003",
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

        mydataOrgInfoWriteRepository.save(entity1)
        mydataOrgInfoWriteRepository.save(entity2)

        val allEntities = mydataOrgInfoWriteRepository.findAll().toList()
        assertTrue(allEntities.size >= 2)

        val savedOrgCodes = allEntities.map { it.orgCode }
        assertTrue(savedOrgCodes.contains("ORG002"))
        assertTrue(savedOrgCodes.contains("ORG003"))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `delete test`() = runTest {
        val entity = MydataOrgInfoEntity(
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

        mydataOrgInfoWriteRepository.save(entity)

        val foundBeforeDelete = mydataOrgInfoWriteRepository.findById("ORG_DELETE")
        assertNotNull(foundBeforeDelete)

        mydataOrgInfoWriteRepository.deleteById("ORG_DELETE")

        val foundAfterDelete = mydataOrgInfoWriteRepository.findById("ORG_DELETE")
        assertNull(foundAfterDelete)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `existsById test`() = runTest {
        val entity = MydataOrgInfoEntity(
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

        val existsBeforeSave = mydataOrgInfoWriteRepository.existsById("ORG_EXISTS")
        assertFalse(existsBeforeSave)

        mydataOrgInfoWriteRepository.save(entity)

        val existsAfterSave = mydataOrgInfoWriteRepository.existsById("ORG_EXISTS")
        assertTrue(existsAfterSave)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `update test`() = runTest {
        val entity = MydataOrgInfoEntity(
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

        mydataOrgInfoWriteRepository.save(entity)

        val updatedEntity = MydataOrgInfoEntity(
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

        val savedUpdatedEntity = mydataOrgInfoWriteRepository.save(updatedEntity)
        assertEquals("Updated Bank", savedUpdatedEntity.orgName)
        assertEquals("Updated Address", savedUpdatedEntity.address)
        assertEquals("updatedbank.com", savedUpdatedEntity.domain)
        assertNull(savedUpdatedEntity.certIssuerDn)
    }
}
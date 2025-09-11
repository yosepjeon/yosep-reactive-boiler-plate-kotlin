package com.yosep.server.common.component.external

import com.yosep.server.common.AbstractIntegrationContainerBase
import com.yosep.server.infrastructure.db.common.entity.WebClientConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.WebClientConfigRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.reactive.function.client.WebClient
import java.time.LocalDateTime

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrgWebClientManagerTest @Autowired constructor(
    private val orgWebClientManager: OrgWebClientManager,
    private val defaultWebClient: WebClient,
    private val webClientConfigRepository: WebClientConfigRepository,
) : AbstractIntegrationContainerBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `getClient는 ID가 존재하면 설정된 클라이언트를, 없으면 기본 클라이언트를 반환한다`() = runTest {
        // given: webclient_config 행 삽입
        val id = "test-org-getClient"
        val entity = WebClientConfigEntity(
            id,
            "https://api.example.test",
            1500,
            2500,
            3500,
            "{\"X-Test\":\"true\"}",
            LocalDateTime.now(),
            LocalDateTime.now(),
            true
        )
        webClientConfigRepository.save(entity)

        println(webClientConfigRepository.findAll().toList())

        // when: 리포지토리에서 클라이언트 초기화
        orgWebClientManager.initClients()

        // then: 존재하는 id에 대해 getClient는 기본이 아닌 클라이언트를 반환
        val configured = orgWebClientManager.getClient(id)
        assertNotSame(defaultWebClient, configured, "Expected a dedicated WebClient for id='$id'")

        // and: 존재하지 않는 id는 기본 WebClient 반환
        val fallback = orgWebClientManager.getClient("unknown-id")
        assertSame(defaultWebClient, fallback, "Unknown id should return default WebClient")
    }
}

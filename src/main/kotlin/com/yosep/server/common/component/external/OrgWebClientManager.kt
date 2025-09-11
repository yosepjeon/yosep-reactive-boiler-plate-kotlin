package com.yosep.server.common.component.external

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.infrastructure.db.common.entity.WebClientConfigEntity
import com.yosep.server.infrastructure.db.common.write.repository.WebClientConfigRepository
import io.netty.channel.ChannelOption
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Component
class OrgWebClientManager(
    private val defaultWebClient: WebClient,
    private val configRepository: WebClientConfigRepository,
) {
    private val log = LoggerFactory.getLogger(OrgWebClientManager::class.java)

    private val job = SupervisorJob()
    private val scope = CoroutineScope(job + Dispatchers.Default)
    private val webClientMap: MutableMap<String, WebClient> = ConcurrentHashMap()
    private val objectMapper = ObjectMapper()

    @PostConstruct
    fun postConstructLog() {
        log.info("OrgWebClientManager bean created")
    }

    @EventListener(ApplicationReadyEvent::class)
    suspend fun onReady() {
        log.info("################!@@@ (coroutine)")

        try {
            initClients()
            log.info("CoroutineOrgWebClientManager initialized {webClientMap.size}: ${webClientMap.size} WebClients")
        } catch (e: Exception) {
            log.error("Failed to initialize CoroutineOrgWebClientManager", e)
        }
    }

    @PreDestroy
    fun shutdown() {
        // Cancel background tasks to avoid leaks
        job.cancel()
    }

    suspend fun initClients() {
        // Load all configs from coroutine repository and build clients
        configRepository.findAll().collect { webClientConfigEntity ->
            val entry = createWebClient(webClientConfigEntity)
            webClientMap[entry.first] = entry.second
        }
    }

    fun getClient(id: String): WebClient {
        return webClientMap[id] ?: defaultWebClient
    }

    private fun createWebClient(webClientConfigEntity: WebClientConfigEntity): Pair<String, WebClient> {
        var httpClient = HttpClient.create()

        webClientConfigEntity.connectTimeoutMs?.let { httpClient = httpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, it) }
        webClientConfigEntity.readTimeoutMs?.let { httpClient = httpClient.responseTimeout(Duration.ofMillis(it.toLong())) }
        // Note: writeTimeout is not applied here as the original Java version didn't set it either.

        httpClient = httpClient
            .doOnConnected { /* placeholder if we need to add handlers later */ }

        val builder = WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))

        webClientConfigEntity.baseUrl?.let { builder.baseUrl(it) }

        val headersJson = webClientConfigEntity.defaultHeaders
        if (!headersJson.isNullOrBlank()) {
            try {
                val headers: Map<String, String> = objectMapper.readValue(
                    headersJson,
                    object : TypeReference<Map<String, String>>() {}
                )
                builder.defaultHeaders { httpHeaders ->
                    headers.forEach { (name, value) -> httpHeaders.set(name, value) }
                }
            } catch (e: Exception) {
                throw IllegalArgumentException("잘못된 헤더 JSON: ${webClientConfigEntity.getId()}", e)
            }
        }

        return webClientConfigEntity.getId() to builder.build()
    }
}

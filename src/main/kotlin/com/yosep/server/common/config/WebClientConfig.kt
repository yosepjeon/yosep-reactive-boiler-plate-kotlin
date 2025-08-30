package com.yosep.server.common.config

import com.yosep.server.common.properties.WebClientProperties
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.http.codec.HttpMessageWriter
import org.springframework.http.codec.LoggingCodecSupport
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@Configuration
class WebClientConfig(private val webClientProperties: WebClientProperties) {
    @Value("\${external.server.url}")
    private val externalServerUrl: String? = null

    @Bean
    fun externalClient(): WebClient {
        val exchangeStrategies = getExchangeStrategies(
            webClientProperties.external!!.byteCnt
        )
        return getWebClient(exchangeStrategies, webClientProperties.external!!)
    }

    fun getExchangeStrategies(byteCnt: Int): ExchangeStrategies {
        val exchangeStrategies = ExchangeStrategies.builder()
            .codecs(Consumer { codecs: ClientCodecConfigurer? -> codecs!!.defaultCodecs().maxInMemorySize(byteCnt) })
            .build()

        exchangeStrategies
            .messageWriters().stream()
            .filter { obj: HttpMessageWriter<*>? -> LoggingCodecSupport::class.java.isInstance(obj) }
            .forEach { writer: HttpMessageWriter<*>? ->
                (writer as LoggingCodecSupport).setEnableLoggingRequestDetails(
                    true
                )
            }

        return exchangeStrategies
    }

    fun getWebClient(
        exchangeStrategies: ExchangeStrategies,
        timoutProperties: WebClientProperties.TimoutProperties
    ): WebClient {
        val connectionProvider = ConnectionProvider.builder("custom")
            .maxConnections(10000) // 최대 연결 수 증가
            .pendingAcquireMaxCount(100000) // 대기 큐 크기 증가
            .pendingAcquireTimeout(Duration.ofSeconds(30)) // 대기 타임아웃 설정
            .build()

        return WebClient.builder()
            .clientConnector(
                ReactorClientHttpConnector(
                    HttpClient
                        .create(connectionProvider)
                        .option<Int?>(
                            ChannelOption.CONNECT_TIMEOUT_MILLIS,
                            timoutProperties.connectionTimeout
                        )
                        .responseTimeout(Duration.ofMillis(timoutProperties.responseTimeout.toLong()))
                        .doOnConnected(Consumer { conn: Connection? ->
                            conn!!.addHandlerLast(
                                ReadTimeoutHandler(
                                    timoutProperties.readTimeout.toLong(),
                                    TimeUnit.MILLISECONDS
                                )
                            )
                                .addHandlerLast(
                                    WriteTimeoutHandler(
                                        timoutProperties.writeTimeout.toLong(),
                                        TimeUnit.MILLISECONDS
                                    )
                                )
                        }
                        )
                )
            )
            .baseUrl(externalServerUrl!!)
            .exchangeStrategies(exchangeStrategies)
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.ALL_VALUE)
            .build()
    }
}
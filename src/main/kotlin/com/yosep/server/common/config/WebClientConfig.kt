package com.yosep.server.common.config
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.http.codec.LoggingCodecSupport
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class WebClientConfig {

    @Value("\${webclient.connection.timeout}")
    private var connectTimeoutMillis: Int = 0

    @Value("\${webclient.response.timeout}")
    private var responseTimeoutMillis: Int = 0

    @Value("\${webclient.read.timeout}")
    private var readTimeoutMillis: Int = 0

    @Value("\${webclient.write.timeout}")
    private var writeTimeoutMillis: Int = 0

    private val log = LoggerFactory.getLogger(WebClientConfig::class.java)

    @Bean
    fun defaultWebClient(): WebClient {
        val exchangeStrategies = ExchangeStrategies.builder()
            // .codecs { acceptedCodecs(it) } // 필요 시 코덱 설정
            .build()
            .also { strategies ->
                strategies.messageWriters()
                    .filterIsInstance<LoggingCodecSupport>()
                    .forEach { it.setEnableLoggingRequestDetails(true) }
            }

        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
            .responseTimeout(Duration.ofMillis(responseTimeoutMillis.toLong()))
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(readTimeoutMillis.toLong(), TimeUnit.MILLISECONDS))
                    .addHandlerLast(WriteTimeoutHandler(writeTimeoutMillis.toLong(), TimeUnit.MILLISECONDS))
            }

        return WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .exchangeStrategies(exchangeStrategies)
            .filter(ExchangeFilterFunction.ofRequestProcessor { request ->
                log.debug("Request: {} {}", request.method(), request.url())
                request.headers().forEach { name, values ->
                    values.forEach { value -> log.debug("{} : {}", name, value) }
                }
                Mono.just(request)
            })
            .filter(ExchangeFilterFunction.ofResponseProcessor { response ->
                response.headers().asHttpHeaders().forEach { name, values ->
                    values.forEach { value -> log.debug("{} : {}", name, value) }
                }
                Mono.just(response)
            })
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .build()
    }
}
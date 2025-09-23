package com.yosep.server.common.config

import io.netty.channel.ChannelOption
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory
import org.springframework.boot.web.embedded.netty.NettyServerCustomizer
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import reactor.netty.http.HttpProtocol
import java.time.Duration

@Configuration
class NettyHttp2Config {

    @Bean
    @Profile("local")
    fun h2cCustomizer(): WebServerFactoryCustomizer<NettyReactiveWebServerFactory> =
        WebServerFactoryCustomizer { factory ->
            factory.addServerCustomizers(
                NettyServerCustomizer { httpServer ->
                    httpServer
                        .protocol(HttpProtocol.H2C)
                        .idleTimeout(Duration.ofMinutes(30))
                        .compress(false)
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .http2Settings { it.maxConcurrentStreams(1024) }
                }
            )
        }

    @Bean
    @Profile("prod")
    fun h2TlsCustomizer(): WebServerFactoryCustomizer<NettyReactiveWebServerFactory> =
        WebServerFactoryCustomizer { factory ->
            factory.addServerCustomizers(
                NettyServerCustomizer { httpServer ->
                    httpServer
                        .protocol(HttpProtocol.H2)
                        .idleTimeout(Duration.ofMinutes(30))
                        .compress(false)
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .http2Settings { it.maxConcurrentStreams(1024) }
                }
            )
        }
}

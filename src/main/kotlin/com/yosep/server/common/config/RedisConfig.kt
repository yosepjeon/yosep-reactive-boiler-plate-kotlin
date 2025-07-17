package com.yosep.server.common.config

import com.yosep.server.common.properties.RedisProperties
import lombok.RequiredArgsConstructor
import org.redisson.Redisson
import org.redisson.api.RedissonReactiveClient
import org.redisson.client.codec.Codec
import org.redisson.client.codec.StringCodec
import org.redisson.config.Config
import org.redisson.config.ReadMode
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(RedisProperties::class)
class RedisConfig (
    private val redisProperties: RedisProperties
){

    @Bean
    fun redissonClient(): RedissonReactiveClient {
        val config = Config()
        val codec: Codec = StringCodec()
        config.setCodec(codec)

        config.useMasterSlaveServers()
            .setMasterAddress(redisProperties.master)
            .addSlaveAddress(redisProperties.slave)
            .setReadMode(ReadMode.SLAVE)

        return Redisson.create(config).reactive()
    }
}
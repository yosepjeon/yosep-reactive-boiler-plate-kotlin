package com.yosep.server.infrastructure.kafka.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.kafka.sender.SenderOptions

@Configuration
class KafkaProducerConfig(
    private val kafkaProperties: KafkaProperties
) {

    private fun producerFactoryConfig(): Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to kafkaProperties.producer.keySerializer,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to kafkaProperties.producer.valueSerializer,
        ProducerConfig.RETRIES_CONFIG to kafkaProperties.producer.retries,
        ProducerConfig.COMPRESSION_TYPE_CONFIG to kafkaProperties.producer.compressionType,
        ProducerConfig.ACKS_CONFIG to kafkaProperties.producer.acks
    )

    private fun byteProducerFactoryConfig(): Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to kafkaProperties.producer.keySerializer,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.RETRIES_CONFIG to kafkaProperties.producer.retries,
        ProducerConfig.COMPRESSION_TYPE_CONFIG to kafkaProperties.producer.compressionType,
        ProducerConfig.ACKS_CONFIG to kafkaProperties.producer.acks
    )

    @Bean
    fun reactiveKafkaProducerTemplate(): ReactiveKafkaProducerTemplate<String, Any> =
        ReactiveKafkaProducerTemplate(SenderOptions.create(producerFactoryConfig()))

    @Bean
    fun reactiveByteKafkaProducerTemplate(): ReactiveKafkaProducerTemplate<String, ByteArray> =
        ReactiveKafkaProducerTemplate(SenderOptions.create(byteProducerFactoryConfig()))
}

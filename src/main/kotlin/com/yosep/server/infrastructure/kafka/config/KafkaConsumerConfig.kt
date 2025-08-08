package com.yosep.server.infrastructure.kafka.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration

@Configuration
class KafkaConsumerConfig(
    private val kafkaProperties: KafkaProperties
) {

    @Value("\${kafka.topic.test.test-1}")
    private lateinit var kafkaTopicTestTest1Consumer: String

    @Bean
    fun test1ConsumerTemplate(): ReactiveKafkaConsumerTemplate<String, ByteArray> =
        ReactiveKafkaConsumerTemplate(createReceiverOptions(kafkaTopicTestTest1Consumer))

    private fun createReceiverOptions(topic: String): ReceiverOptions<String, ByteArray> =
        ReceiverOptions.create<String, ByteArray>(consumerFactoryProperties())
            .subscription(listOf(topic))

    private fun createBatchReceiverOptions(topic: String): ReceiverOptions<String, ByteArray> =
        ReceiverOptions.create<String, ByteArray>(consumerFactoryProperties())
            .subscription(listOf(topic))
            .commitInterval(Duration.ofSeconds(1))   // 배치 ack 시 커밋 주기
            .commitBatchSize(100)

    private fun consumerFactoryProperties(): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to kafkaProperties.consumer.groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to kafkaProperties.consumer.keyDeserializer,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to kafkaProperties.consumer.valueDeserializer,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to kafkaProperties.consumer.enableAutoCommit,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to kafkaProperties.consumer.maxPollRecords,
        JsonDeserializer.TRUSTED_PACKAGES to "*",
        JsonDeserializer.VALUE_DEFAULT_TYPE to Object::class.java,
        JsonDeserializer.TYPE_MAPPINGS to ""
    )
}

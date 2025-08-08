package com.yosep.server.infrastructure.kafka.config

import lombok.Getter
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Getter
@Configuration
class KafkaTopicConfig {
    @Value("\${spring.kafka.bootstrap-servers}")
    private val bootstrapAddress: String? = null

    @Value("\${kafka.topic.test.test-1}")
    private lateinit var kafkaTopicTestTest1Consumer: String

    // TODO 레거시 금융 마이데이터 운영 완료시 삭제 예정
    @Bean
    fun kafkaTopicKcbCreditIncreaseRequestProducer(): NewTopic {
        return TopicBuilder.name(kafkaTopicTestTest1Consumer)
            .partitions(3)
            .replicas(3)
            .build()
    }
}
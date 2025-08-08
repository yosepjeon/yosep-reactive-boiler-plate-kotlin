package com.yosep.server.infrastructure.kafka.component

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.stereotype.Component
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult

@Component
class KafkaByteMessageSender(
    @Qualifier("reactiveByteKafkaProducerTemplate")
    private val reactiveByteKafkaProducerTemplate: ReactiveKafkaProducerTemplate<String, ByteArray>
) {

    suspend fun send(topic: String, message: ByteArray): SenderResult<Any> {
        val record: SenderRecord<String?, ByteArray?, Any?> =
            SenderRecord.create(topic, null, null, null, message, null)
        @Suppress("UNCHECKED_CAST")
        return reactiveByteKafkaProducerTemplate.send(record)
            .awaitSingle() as SenderResult<Any>
    }
}

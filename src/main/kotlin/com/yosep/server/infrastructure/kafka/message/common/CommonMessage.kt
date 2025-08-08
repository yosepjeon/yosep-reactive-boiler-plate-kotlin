package com.yosep.server.infrastructure.kafka.message.common

import com.yosep.server.common.date.YosepDateFormatter
import com.yosep.server.infrastructure.kafka.type.KafkaDataType
import java.time.LocalDateTime

data class CommonMessage<T>(
    val data: T,
    val metadata: MetaData
) {
    constructor(data: T, kafkaDataType: KafkaDataType) : this(
        data = data,
        metadata = MetaData(kafkaDataType)
    )

    data class MetaData(
        var timestamp: String = "",
        var recordType: String? = null,
        var target: String? = null,
        var eventSource: String? = null,
        var eventName: String? = null
    ) {
        constructor(kafkaDataType: KafkaDataType) : this(
            timestamp = LocalDateTime.now().format(YosepDateFormatter.yyyyMmDdHhMmSsFormatter),
            recordType = kafkaDataType.recordType,
            target = kafkaDataType.target,
            eventSource = kafkaDataType.eventSource,
            eventName = kafkaDataType.eventName
        )
    }
}
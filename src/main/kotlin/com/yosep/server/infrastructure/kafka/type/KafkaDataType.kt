package com.yosep.server.infrastructure.kafka.type

const val EVENT = "event"
const val CREDIT = "credit"

enum class KafkaDataType(
    val recordType: String,
    val target: String,
    val eventSource: String,
    val eventName: String
) {
    TEST_REQUEST(
        EVENT,
        KafkaTargetType.TEST.targetName,
        CREDIT,
        "test"
    )
}
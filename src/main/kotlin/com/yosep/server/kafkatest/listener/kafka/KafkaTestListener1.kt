package com.yosep.server.kafkatest.listener.kafka

import com.yosep.server.common.component.YosepDataParser
import com.yosep.server.infrastructure.kafka.config.KafkaTopicConfig
import com.yosep.server.infrastructure.kafka.message.common.CommonMessage
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.configurationprocessor.json.JSONObject
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.stereotype.Component

@Component
class KafkaTestListener1(
    @Qualifier("test1ConsumerTemplate")
    private val test1ConsumerTemplate: ReactiveKafkaConsumerTemplate<String, ByteArray>,
    private val kafkaTopicConfig: KafkaTopicConfig,
    private val yosepDataParser: YosepDataParser
): ApplicationRunner {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("KafkaTestListener1"))

    override fun run(args: ApplicationArguments) {
        // Spring Boot 애플리케이션이 모두 초기화된 후 호출됨
        scope.launch {
        }
    }

    private suspend fun consumeCreditIncreaseRequests() {
        try {
            test1ConsumerTemplate.receive().asFlow().collect { consumerRecord ->
                try {
                    consumerRecord.receiverOffset().acknowledge()

                    val commonMessage = yosepDataParser.parse(consumerRecord.value(), CommonMessage::class.java)
                    val requestMessage = yosepDataParser.parse(
                        commonMessage.data,
                        JSONObject::class.java
                    )


                } catch (ex: Exception) {
                    handleError(ex)
                }
            }
        } catch (e: Exception) {
            handleError(e)
            restartListener()
        }
    }

    private fun handleError(ex: Exception) {
//        val log = ErrorMessageLog(ex)
//        LogUtil.writeErrorMessage(log)
//        LogUtil.writeErrorAlertLog(log)
    }

    private fun restartListener() {
        scope.launch {
            consumeCreditIncreaseRequests()
        }
    }

    @PreDestroy
    fun clearListener() {
        scope.cancel()
    }
}
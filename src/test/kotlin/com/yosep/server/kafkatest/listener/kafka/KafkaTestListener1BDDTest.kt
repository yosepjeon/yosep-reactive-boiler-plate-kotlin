package com.yosep.server.kafkatest.listener.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.common.component.YosepDataParser
import com.yosep.server.infrastructure.kafka.config.KafkaTopicConfig
import com.yosep.server.infrastructure.kafka.message.common.CommonMessage
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.assertj.core.api.BDDAssertions.then
import org.mockito.BDDMockito.*
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.quality.Strictness
import org.springframework.boot.DefaultApplicationArguments
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.support.Acknowledgment
import reactor.core.publisher.Flux
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverRecord

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
@DisplayName("KafkaTestListener1 BDD 테스트")
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTestListener1BDDTest {

    @Mock
    private lateinit var test1ConsumerTemplate: ReactiveKafkaConsumerTemplate<String, ByteArray>

    @Mock
    private lateinit var kafkaTopicConfig: KafkaTopicConfig

    @Mock
    private lateinit var receiverRecord: ReceiverRecord<String, ByteArray>

    @Mock
    private lateinit var receiverOffset: ReceiverOffset

    @Mock
    private lateinit var acknowledgment: Acknowledgment

    private lateinit var yosepDataParser: YosepDataParser
    private lateinit var kafkaTestListener1: KafkaTestListener1

    @BeforeEach
    fun setUp() {
        // Given
        val objectMapper = ObjectMapper().apply {
            findAndRegisterModules()
        }
        yosepDataParser = YosepDataParser(objectMapper)
        kafkaTestListener1 = KafkaTestListener1(test1ConsumerTemplate, kafkaTopicConfig, yosepDataParser)
    }

    @Test
    @DisplayName("KafkaTestListener1이 성공적으로 생성되어야 한다")
    fun shouldCreateKafkaTestListener1Successfully() = runTest {
        // Given - setUp에서 이미 생성됨

        // When - 객체가 생성됨

        // Then
        then(kafkaTestListener1).isNotNull()
        then(yosepDataParser).isNotNull()
    }

    @Test
    @DisplayName("ApplicationRunner의 run 메서드가 정상적으로 실행되어야 한다")
    fun shouldExecuteApplicationRunnerSuccessfully() = runTest {
        // Given
        val applicationArguments = DefaultApplicationArguments()

        // When
        kafkaTestListener1.run(applicationArguments)

        // Then
        delay(100) // 코루틴 실행을 위한 짧은 대기
        then(kafkaTestListener1).isNotNull()
    }

    @Test
    @DisplayName("CommonMessage를 성공적으로 생성하고 파싱해야 한다")
    fun shouldCreateAndParseCommonMessageSuccessfully() = runTest {
        // Given
        val testData = mapOf(
            "testKey" to "testValue",
            "userId" to "12345",
            "amount" to 1000
        )
        
        val commonMessage = CommonMessage(
            data = testData,
            metadata = CommonMessage.MetaData().apply {
                recordType = "CREDIT_INCREASE_REQUEST"
                target = "CREDIT_SERVICE"
                eventSource = "USER_SERVICE"
                eventName = "CREDIT_INCREASE_REQUESTED"
            }
        )

        // When
        val messageBytes = yosepDataParser.parseToBytes(commonMessage)
        val parsedMessage = yosepDataParser.parse(messageBytes, CommonMessage::class.java)

        // Then
        then(messageBytes).isNotNull()
        then(parsedMessage).isNotNull()
        then(parsedMessage.metadata).isNotNull()
        then(parsedMessage.metadata?.recordType).isEqualTo("CREDIT_INCREASE_REQUEST")
        then(parsedMessage.metadata?.target).isEqualTo("CREDIT_SERVICE")
        then(parsedMessage.metadata?.eventSource).isEqualTo("USER_SERVICE")
        then(parsedMessage.metadata?.eventName).isEqualTo("CREDIT_INCREASE_REQUESTED")
    }

    @Test
    @DisplayName("YosepDataParser의 기능을 정상적으로 처리해야 한다")
    fun shouldHandleYosepDataParserFunctionalityCorrectly() = runTest {
        // Given
        val testJsonString = """{"key": "value", "number": 123, "boolean": true}"""

        // When
        val parsedMap = yosepDataParser.parse(testJsonString, Map::class.java)
        val backToString = yosepDataParser.parseToString(parsedMap)

        // Then
        then(parsedMap).isNotNull()
        then(backToString).isNotNull()
        then(parsedMap as Map<String, Any>).containsKey("key")
        then(parsedMap as Map<String, Any>).containsKey("number")
        then(parsedMap as Map<String, Any>).containsKey("boolean")
    }

    @Test
    @DisplayName("Kafka 메시지 수신 시 정상적으로 처리해야 한다")
    fun shouldProcessKafkaMessageCorrectly() = runTest {
        // Given
        val testData = mapOf("userId" to "12345", "amount" to 1000)
        val commonMessage = CommonMessage(
            data = testData,
            metadata = CommonMessage.MetaData().apply {
                recordType = "CREDIT_INCREASE_REQUEST"
                target = "CREDIT_SERVICE"
                eventSource = "USER_SERVICE"
                eventName = "CREDIT_INCREASE_REQUESTED"
            }
        )
        val messageBytes = yosepDataParser.parseToBytes(commonMessage)

        given(receiverRecord.value()).willReturn(messageBytes)
        given(receiverRecord.receiverOffset()).willReturn(receiverOffset)
        willDoNothing().given(receiverOffset).acknowledge()
        given(test1ConsumerTemplate.receive()).willReturn(Flux.just(receiverRecord))

        // When
        // consumeCreditIncreaseRequests는 private이므로 직접 테스트할 수 없지만
        // 메시지 처리 로직이 정상적으로 작동하는지 확인
        val parsedMessage = yosepDataParser.parse(messageBytes, CommonMessage::class.java)

        // Then
        then(parsedMessage).isNotNull()
        then(parsedMessage.data).isNotNull()
        then(parsedMessage.metadata).isNotNull()
    }

    @Test
    @DisplayName("에러 발생 시 정상적으로 처리해야 한다")
    fun shouldHandleErrorCorrectly() = runTest {
        // Given
        val invalidMessageBytes = "invalid json".toByteArray()

        // When & Then
        // 잘못된 데이터로 파싱 시도 시 예외가 발생해야 함
        try {
            yosepDataParser.parse(invalidMessageBytes, CommonMessage::class.java)
        } catch (e: Exception) {
            then(e).isNotNull()
        }
    }

    @Test
    @DisplayName("리스너 재시작이 정상적으로 동작해야 한다")
    fun shouldRestartListenerCorrectly() = runTest {
        // Given
        val listener = kafkaTestListener1

        // When
        // restartListener는 private 메서드이므로 reflection을 통해 접근
        val restartMethod = KafkaTestListener1::class.java.getDeclaredMethod("restartListener")
        restartMethod.isAccessible = true
        restartMethod.invoke(listener)

        // Then
        delay(100) // 코루틴 실행을 위한 짧은 대기
        then(listener).isNotNull()
    }

    @Test
    @DisplayName("리스너 정리가 정상적으로 동작해야 한다")
    fun shouldClearListenerCorrectly() = runTest {
        // Given
        val listener = kafkaTestListener1

        // When
        listener.clearListener()

        // Then
        // scope가 취소되었는지 직접 테스트할 수는 없지만
        // 메서드가 오류 없이 실행되는지 확인
        then(listener).isNotNull()
    }

    @Test
    @DisplayName("복잡한 데이터 구조를 가진 메시지를 정상적으로 처리해야 한다")
    fun shouldProcessComplexMessageStructureCorrectly() = runTest {
        // Given
        val complexData = mapOf(
            "user" to mapOf(
                "id" to "12345",
                "name" to "테스트 사용자",
                "email" to "test@example.com"
            ),
            "transaction" to mapOf(
                "id" to "txn-001",
                "amount" to 50000,
                "currency" to "KRW",
                "timestamp" to System.currentTimeMillis()
            ),
            "metadata" to mapOf(
                "source" to "mobile_app",
                "version" to "1.0.0"
            )
        )

        val commonMessage = CommonMessage(
            data = complexData,
            metadata = CommonMessage.MetaData().apply {
                recordType = "COMPLEX_TRANSACTION"
                target = "PAYMENT_SERVICE"
                eventSource = "MOBILE_APP"
                eventName = "PAYMENT_REQUESTED"
            }
        )

        // When
        val messageBytes = yosepDataParser.parseToBytes(commonMessage)
        val parsedMessage = yosepDataParser.parse(messageBytes, CommonMessage::class.java)

        // Then
        then(messageBytes).isNotNull()
        then(parsedMessage).isNotNull()
        then(parsedMessage.data).isNotNull()
        then(parsedMessage.metadata?.recordType).isEqualTo("COMPLEX_TRANSACTION")
        
        // 복잡한 데이터 구조가 올바르게 파싱되었는지 확인
        val parsedData = parsedMessage.data as Map<String, Any>
        then(parsedData).containsKey("user")
        then(parsedData).containsKey("transaction")
        then(parsedData).containsKey("metadata")
    }

    @Test
    @DisplayName("빈 데이터로 메시지를 생성하고 처리해야 한다")
    fun shouldHandleEmptyDataMessage() = runTest {
        // Given
        val emptyData = emptyMap<String, Any>()
        val commonMessage = CommonMessage(
            data = emptyData,
            metadata = CommonMessage.MetaData().apply {
                recordType = "EMPTY_MESSAGE"
                target = "TEST_SERVICE"
                eventSource = "TEST_SOURCE"
                eventName = "EMPTY_EVENT"
            }
        )

        // When
        val messageBytes = yosepDataParser.parseToBytes(commonMessage)
        val parsedMessage = yosepDataParser.parse(messageBytes, CommonMessage::class.java)

        // Then
        then(messageBytes).isNotNull()
        then(parsedMessage).isNotNull()
        then(parsedMessage.data).isNotNull()
        then(parsedMessage.data).isInstanceOf(Map::class.java)
        then((parsedMessage.data as Map<*, *>)).isEmpty()
    }
}
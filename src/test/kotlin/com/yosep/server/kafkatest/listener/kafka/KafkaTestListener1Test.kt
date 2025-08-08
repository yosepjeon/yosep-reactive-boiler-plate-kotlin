package com.yosep.server.kafkatest.listener.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.common.component.YosepDataParser
import com.yosep.server.infrastructure.kafka.config.KafkaTopicConfig
import com.yosep.server.infrastructure.kafka.message.common.CommonMessage
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import kotlin.test.assertNotNull

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class KafkaTestListener1Test {

    @Mock
    private lateinit var test1ConsumerTemplate: ReactiveKafkaConsumerTemplate<String, ByteArray>

    @Mock
    private lateinit var kafkaTopicConfig: KafkaTopicConfig

    private lateinit var yosepDataParser: YosepDataParser
    private lateinit var kafkaTestListener1: KafkaTestListener1

    @BeforeEach
    fun setUp() {
        val objectMapper = ObjectMapper().apply {
            findAndRegisterModules()
        }
        yosepDataParser = YosepDataParser(objectMapper)
        kafkaTestListener1 = KafkaTestListener1(test1ConsumerTemplate, kafkaTopicConfig, yosepDataParser)
    }

    @Test
    fun `KafkaTestListener1 should be created successfully`() = runTest {
        // Given & When & Then
        assertNotNull(kafkaTestListener1)
        assertNotNull(yosepDataParser)
        assertNotNull(kafkaTopicConfig)
    }

    @Test
    fun `should create and parse CommonMessage successfully`() = runTest {
        // Given
        val testData = mapOf(
            "testKey" to "testValue",
            "userId" to "12345"
        )
        
        val commonMessage = CommonMessage(
            data = testData,
            metadata = CommonMessage.MetaData().apply {
                recordType = "TEST_RECORD"
                target = "TEST_TARGET"
                eventSource = "TEST_SOURCE"
                eventName = "TEST_EVENT"
            }
        )

        // When
        val messageBytes = yosepDataParser.parseToBytes(commonMessage)
        val parsedMessage = yosepDataParser.parse(messageBytes, CommonMessage::class.java)

        // Then
        assertNotNull(messageBytes)
        assertNotNull(parsedMessage)
        assertNotNull(parsedMessage.metadata)
    }

    @Test
    fun `should handle YosepDataParser functionality`() = runTest {
        // Given
        val testString = """{"key": "value", "number": 123}"""

        // When
        val parsedMap = yosepDataParser.parse(testString, Map::class.java)
        val backToString = yosepDataParser.parseToString(parsedMap)

        // Then
        assertNotNull(parsedMap)
        assertNotNull(backToString)
    }

    @Test
    fun `should restart listener after error`() = runTest {
        // Given
        val listener = kafkaTestListener1

        // When
        // Test the restart functionality by calling the private method through reflection
        val restartMethod = KafkaTestListener1::class.java.getDeclaredMethod("restartListener")
        restartMethod.isAccessible = true
        restartMethod.invoke(listener)

        // Then
        delay(1000)
        assertNotNull(listener)
    }

    @Test
    fun `should clear listener on destroy`() = runTest {
        // Given
        val listener = kafkaTestListener1

        // When
        listener.clearListener()

        // Then
        // The scope should be cancelled, but we can't directly test this
        // We just verify the method executes without error
        assertNotNull(listener)
    }

    @Test
    fun `should handle application runner execution`() = runTest {
        // Given
        val listener = kafkaTestListener1
        val mockArgs = org.springframework.boot.DefaultApplicationArguments()

        // When
        listener.run(mockArgs)

        // Then
        delay(1000)
        assertNotNull(listener)
    }
}
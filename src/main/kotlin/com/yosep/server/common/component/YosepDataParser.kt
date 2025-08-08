package com.yosep.server.common.component

import com.fasterxml.jackson.databind.ObjectMapper
import com.yosep.server.common.exception.YosepErrorException
import com.yosep.server.infrastructure.kafka.message.common.CommonMessage
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import org.springframework.util.ObjectUtils

@Component
class YosepDataParser (
    private val objectMapper: ObjectMapper
) {

    fun <R> parse(data: ByteArray, clazz: Class<R>): R {
        return try {
            objectMapper.readValue(data, clazz)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <R> parse(data: String, clazz: Class<R>): R {
        return try {
            objectMapper.readValue(data, clazz)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <R> parse(data: ClassPathResource, clazz: Class<R>): R {
        return try {
            data.inputStream.use { inputStream ->
                objectMapper.readValue(inputStream, clazz)
            }
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <R> parse(data: Any?, clazz: Class<R>): R {
        return objectMapper.convertValue(data, clazz)
    }

    fun <T> parseToBytes(data: T): ByteArray {
        return try {
            objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <T> parseToString(data: T): String {
        return try {
            objectMapper.writeValueAsString(data)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <T> parseObjectToString(data: T?): String? {
        return try {
            if (ObjectUtils.isEmpty(data)) null else objectMapper.writeValueAsString(data)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }

    fun <T> parseMessageToBytes(data: CommonMessage<T>): ByteArray {
        return try {
            objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw YosepErrorException(e)
        }
    }
}
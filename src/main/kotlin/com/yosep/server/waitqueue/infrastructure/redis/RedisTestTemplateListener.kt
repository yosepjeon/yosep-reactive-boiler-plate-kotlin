//package com.yosep.server.waitqueue.infrastructure.redis
//
//import com.yosep.server.common.component.YosepDataParser
//import com.yosep.server.infrastructure.kafka.message.common.message.CommonMessage
//import jakarta.annotation.PostConstruct
//import kotlinx.coroutines.CoroutineScope
//import kotlinx.coroutines.Dispatchers
//import kotlinx.coroutines.launch
//import org.redisson.api.RedissonReactiveClient
//import org.springframework.beans.factory.annotation.Value
//import org.springframework.stereotype.Component
//
//@Component
//class RedisTestTemplateListener (
//    private val yosepDataParser: YosepDataParser,
//    private val redissonReactiveClient: RedissonReactiveClient
//) {
//
//    @Value("\${redis.topic.kcb.credit-increase-result.consumer}")
//    private lateinit var redisTopicKcbCreditIncreaseResultConsumer: String
//
//    private val coroutineScope = CoroutineScope(Dispatchers.IO)
//
//    @PostConstruct
//    fun init() {
//        redissonReactiveClient.getTopic(redisTopicKcbCreditIncreaseResultConsumer)
//            .addListener(String::class.java) { _, message ->
//                coroutineScope.launch {
//                    handleMessage(message)
//                }
//            }
//    }
//
//    private suspend fun handleMessage(message: String) {
//        var txId: String? = null
//        var userId: Long? = null
//        try {
//            val commonMessage = yosepDataParser.parse(message, CommonMessage::class.java)
//            println("####################")
//            println(commonMessage)
//
//        } catch (e: Exception) {
//            processListenerError(txId, userId, e)
//        }
//    }
//
//    private fun processListenerError(txId: String?, userId: Long?, exception: Exception) {
////        if (profileService.isNotPrd()) {
////            exception.printStackTrace()
////        }
//
////        CreditLogUtil.writeErrorMessage(ErrorMessageLog(txId, userId, exception))
////        if (!profileService.isStg() &&
////            exception !is FindaClientException &&
////            exception !is DataIntegrityViolationException
////        ) {
////            CreditLogUtil.writeErrorAlertLog(ErrorMessageLog(txId, userId, exception))
////        }
//    }
//}
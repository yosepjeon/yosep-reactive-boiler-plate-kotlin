package com.yosep.server.common.exception


open class YosepErrorException : RuntimeException {

    val messageText: String?

    constructor(message: String?, cause: Throwable?) : super(message, cause) {
        messageText = message
    }

    constructor(cause: Throwable) : super(cause) {
        messageText = cause.rootCauseMessage()
    }
}

// 확장 함수로 Exception rootCauseMessage 대체
fun Throwable.rootCauseMessage(): String {
    var root: Throwable = this
    while (root.cause != null) {
        root = root.cause!!
    }
    return root.message ?: this.message ?: ""
}
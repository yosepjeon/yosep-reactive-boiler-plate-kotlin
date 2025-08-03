package com.yosep.server.common.exception


class YosepErrorException : RuntimeException {

    val messageText: String

    constructor(message: String, cause: Throwable?) : super(message, cause) {
        messageText = message
    }

    constructor(message: String) : this(message, null)

    constructor(cause: Throwable) : super(cause) {
        messageText = cause.rootCauseMessage()
    }

    constructor() : super() {
        messageText = ""
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
package com.yosep.server.common.exception

import org.springframework.http.HttpStatus

open class HttpErrorException : YosepErrorException {
    var httpStatus: HttpStatus? = HttpStatus.INTERNAL_SERVER_ERROR

    @JvmOverloads
    constructor(message: String?, httpStatus: HttpStatus?, e: Throwable? = null) : super(message, e) {
        this.httpStatus = httpStatus
    }

    constructor(message: String?, e: Throwable?) : super(message, e)
}
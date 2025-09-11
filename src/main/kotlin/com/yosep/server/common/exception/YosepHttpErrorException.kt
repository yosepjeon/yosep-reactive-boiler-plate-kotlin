package com.yosep.server.common.exception

import com.yosep.server.domain.external.ApiRequest
import lombok.Getter
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType

@Getter
class YosepHttpErrorException(
    message: String?, private val body: Any?,
    httpStatus: HttpStatus?, e: Throwable?, request: ApiRequest<*>?, mediaType: MediaType?
) : HttpErrorException(message, httpStatus, e) {
    private val request: ApiRequest<*>?
    private val mediaType: MediaType?

    init {
        this.request = request
        this.mediaType = mediaType
    }

    constructor(
        body: Any?,
        httpStatus: HttpStatus, mediaType: MediaType?, request: ApiRequest<*>?
    ) : this(httpStatus.getReasonPhrase(), body, httpStatus, null, request, mediaType)

    constructor(
        message: String?, body: Any?,
        httpStatus: HttpStatus?, mediaType: MediaType?, request: ApiRequest<*>?
    ) : this(message, body, httpStatus, null, request, mediaType)
}
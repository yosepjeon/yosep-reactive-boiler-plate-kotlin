package com.yosep.server.domain.external

import com.fasterxml.jackson.annotation.JsonIgnore
import org.springframework.http.HttpMethod
import org.springframework.http.RequestEntity
import org.springframework.util.LinkedMultiValueMap
import java.net.URI

class ApiRequest<T> (
    var body: T? = null,
    var headers: LinkedMultiValueMap<String?, String?>? = null,
    var code: String? = null,
    var orgCode: String? = null,
    var domain: String? = null,
    var resource: String? = null,
    var method: HttpMethod? = null,
    var proxyUrl: URI? = null,
    var userId: Long? = null,

) {
    fun getHeaderFirst(key: String): String? {
        return headers!!.getFirst(key)
    }

    @get:JsonIgnore
    val request: RequestEntity<T?>
        get() = RequestEntity<T?>(body, headers, method, proxyUrl!!)
}
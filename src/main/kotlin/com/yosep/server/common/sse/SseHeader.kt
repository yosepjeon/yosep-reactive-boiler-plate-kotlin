package com.yosep.server.common.sse

import org.springframework.http.HttpHeaders

/**
 * SSE 응답 헤더 유틸리티
 */
object SseHeader {
    fun get(): HttpHeaders {
        return HttpHeaders().apply {
            add("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate")
            add("Pragma", "no-cache")
            add("Expires", "0")
            add("X-Accel-Buffering", "no") // Nginx 버퍼링 비활성화
            add("Connection", "keep-alive")
        }
    }
}
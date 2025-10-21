package com.yosep.server.common.component.ratelimit.local

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "ratelimit.local")
class LocalRateLimitProperties {
    var enabled: Boolean = false
    var windowMs: Long = 1000
    var latencyThresholdMs: Long = 500
    var syncIntervalMs: Long = 1000
    var enableSync: Boolean = true
    var minPerNode: Int = 1
    var nodeCountCacheMs: Long = 5000
    var counterTtlSec: Long = 60

    // AIMD parameters
    var maxLimit: Int = 10000
    var minLimit: Int = 10
    var failureThresholdMs: Long = 500
    var failureMd: Double = 0.5
    var failureNThreshold: Int = 3

    // Atomic version specific
    var atomic: AtomicProperties = AtomicProperties()

    var targetP99Ms: Long = 500        // 목표 p99
    var addStep: Int = 50              // 정상 시 증가폭
    var decreaseFactor: Double = 0.7   // 초과 시 감소 계수
    var reservoirSize: Int = 4096
    var ewmaTauMs: Long = 3000         // EWMA 시간상수(τ), 3s 권장
    var hysteresisMs: Long = 30        // 히스테리시스 완충(±30ms 구간에서는 limit 고정)
    var minSamplesForP99: Int = 64     // p99 계산 최소 샘플 수
    var latencyWindowMs: Long = 10_000L

    class AtomicProperties {
        var enabled: Boolean = false
    }
}
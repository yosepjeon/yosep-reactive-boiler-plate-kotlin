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

    class AtomicProperties {
        var enabled: Boolean = false
    }
}
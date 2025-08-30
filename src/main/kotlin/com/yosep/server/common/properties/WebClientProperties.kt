package com.yosep.server.common.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@ConfigurationProperties(prefix = "webclient")
@Configuration
class WebClientProperties {
    var external: TimoutProperties? = null

    class TimoutProperties {
        var connectionTimeout: Int = 0
        var responseTimeout: Int = 0
        var readTimeout: Int = 0
        var writeTimeout: Int = 0
        var byteCnt: Int = 0
    }
}
package com.yosep.server.common.properties


import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "webclient")
class WebClientProperties {
    var kcb: TimeoutProperties? = null
    var mydata: TimeoutProperties? = null

    data class TimeoutProperties(
        var connectionTimeout: Int = 0,
        var responseTimeout: Int = 0,
        var readTimeout: Int = 0,
        var writeTimeout: Int = 0,
        var byteCnt: Int = 0
    )
}
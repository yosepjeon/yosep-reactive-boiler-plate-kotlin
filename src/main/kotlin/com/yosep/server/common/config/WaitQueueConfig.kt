package com.yosep.server.common.config

import com.yosep.server.common.component.waitqueue.WaitQueueProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(WaitQueueProperties::class)
class WaitQueueConfig
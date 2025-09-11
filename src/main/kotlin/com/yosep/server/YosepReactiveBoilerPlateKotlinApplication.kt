package com.yosep.server

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class YosepReactiveBoilerPlateKotlinApplication

fun main(args: Array<String>) {
	runApplication<YosepReactiveBoilerPlateKotlinApplication>(*args)
}

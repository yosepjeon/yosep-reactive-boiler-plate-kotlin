package com.yosep.server

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = ["com.yosep.server"])
class YosepReactiveBoilerPlateKotlinApplication

fun main(args: Array<String>) {
	runApplication<YosepReactiveBoilerPlateKotlinApplication>(*args)
}

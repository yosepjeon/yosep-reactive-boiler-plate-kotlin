package com.yosep.server

import com.yosep.server.common.AbstractIntegrationContainerBase
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YosepReactiveBoilerPlateKotlinApplicationTests : AbstractIntegrationContainerBase() {

	@Test
	fun contextLoads() {
	}

}

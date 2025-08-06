package com.yosep.server.common

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Ports
import com.github.dockerjava.api.model.Ports.Binding
import org.junit.jupiter.api.BeforeAll
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.wait.strategy.Wait

abstract class AbstractIntegrationContainerBase {

    companion object {

        private val redisContainer = GenericContainer(DockerImageName.parse("redis:latest"))
            .apply {
                withExposedPorts(6379)
                withCreateContainerCmdModifier { cmd ->
                    val ports = Ports()
                    ports.bind(ExposedPort.tcp(6379), Binding.bindPort(56379))
                    val hostConfig = HostConfig.newHostConfig().withPortBindings(ports)
                    cmd.withHostConfig(hostConfig)
                }
                waitingFor(Wait.forListeningPort())
                withReuse(false)
            }

        private val mysqlContainer = MySQLContainer(DockerImageName.parse("mysql:8.0.33"))
            .apply {
                withDatabaseName("testdb")
                withUsername("testuser")
                withPassword("testpass")
                withExposedPorts(3306)
                withCreateContainerCmdModifier { cmd ->
                    val ports = Ports()
                    ports.bind(ExposedPort.tcp(3306), Binding.bindPort(53306))
                    val hostConfig = HostConfig.newHostConfig().withPortBindings(ports)
                    cmd.withHostConfig(hostConfig)
                }
                waitingFor(Wait.forListeningPort())
                withReuse(false)
            }

        private val mongoContainer = MongoDBContainer(DockerImageName.parse("mongo:latest"))
            .apply {
                withExposedPorts(27017)
                withCreateContainerCmdModifier { cmd ->
                    val ports = Ports()
                    ports.bind(ExposedPort.tcp(27017), Binding.bindPort(57017))
                    val hostConfig = HostConfig.newHostConfig().withPortBindings(ports)
                    cmd.withHostConfig(hostConfig)
                }
                waitingFor(Wait.forListeningPort())
                withReuse(false)
            }

        private val elasticsearchContainer =
            ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.10")
                .apply {
                    withEnv("xpack.security.enabled", "false")
                    withEnv("discovery.type", "single-node")
                    withExposedPorts(9200, 9300)
                    withCreateContainerCmdModifier { cmd ->
                        val ports = Ports()
                        ports.bind(ExposedPort.tcp(9200), Binding.bindPort(59200))
                        ports.bind(ExposedPort.tcp(9300), Binding.bindPort(59300))
                        val hostConfig = HostConfig.newHostConfig().withPortBindings(ports)
                        cmd.withHostConfig(hostConfig)
                    }
                    waitingFor(Wait.forListeningPort())
                    withReuse(false)
                }

        @JvmStatic
        @BeforeAll
        fun startContainers() {
            redisContainer.start()
            mysqlContainer.start()
            mongoContainer.start()
            elasticsearchContainer.start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun overrideProperties(registry: DynamicPropertyRegistry) {
            // Redis
            registry.add("spring.data.redis.host") { redisContainer.host }
            registry.add("spring.data.redis.port") { 56379 }

            // MySQL(JDBC)
            registry.add("spring.datasource.url") {
                "jdbc:mysql://${mysqlContainer.host}:53306/${mysqlContainer.databaseName}"
            }
            registry.add("spring.datasource.username") { mysqlContainer.username }
            registry.add("spring.datasource.password") { mysqlContainer.password }

            // MySQL (R2DBC)
            registry.add("spring.r2dbc.url") {
                "r2dbc:mysql://${mysqlContainer.host}:53306/${mysqlContainer.databaseName}"
            }
            registry.add("spring.r2dbc.username") { mysqlContainer.username }
            registry.add("spring.r2dbc.password") { mysqlContainer.password }

            // MongoDB
            registry.add("spring.data.mongodb.uri") {
                "mongodb://${mongoContainer.host}:57017/testdb"
            }

            // Elasticsearch
            registry.add("spring.elasticsearch.uris") {
                "http://${elasticsearchContainer.host}:59200"
            }

            // Flyway
            registry.add("spring.flyway.enabled") { true }
            registry.add("spring.flyway.clean-disabled") { false }
            registry.add("spring.flyway.locations") { "classpath:db/migration" }
        }
    }
}

package com.yosep.server.common

import com.yosep.server.infrastructure.db.common.write.repository.CircuitBreakerConfigWriteRepository
import com.yosep.server.infrastructure.db.common.write.repository.OrgInfoWriteRepository
import com.yosep.server.infrastructure.db.common.write.repository.OrgRateLimitConfigWriteRepository
import com.yosep.server.infrastructure.db.common.write.repository.WebClientConfigRepository
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Assumptions
import org.redisson.api.RedissonReactiveClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration

abstract class AbstractIntegrationContainerBase {

    @Autowired(required = false)
    private var databaseClient: DatabaseClient? = null

    @Autowired(required = false)
    private var redissonReactiveClient: RedissonReactiveClient? = null

    @Autowired(required = false)
    private var circuitBreakerConfigWriteRepository: CircuitBreakerConfigWriteRepository? = null

    @Autowired(required = false)
    private var orgRateLimitConfigWriteRepository: OrgRateLimitConfigWriteRepository? = null

    @Autowired(required = false)
    private var webClientConfigRepository: WebClientConfigRepository? = null

    @Autowired(required = false)
    private var orgInfoWriteRepository: OrgInfoWriteRepository? = null

    @AfterEach
    open fun cleanupTestData() {
        runBlocking {
            try {
                // Redis 테스트 데이터 정리
                cleanupRedisData()

                // 리포지토리를 사용해 DB 테스트 데이터 정리
                cleanupDatabaseTables()

                println("[DEBUG_LOG] Test data cleanup completed successfully")
            } catch (e: Exception) {
                println("[DEBUG_LOG] Error during test data cleanup: ${e.message}")
                // 테스트 실행이 중단되지 않도록 예외를 다시 던지지 않음
            }
        }
    }

    private suspend fun cleanupDatabaseTables() {
        try {
            // 리포지토리를 사용해 테이블 테스트 데이터 삭제
            circuitBreakerConfigWriteRepository?.deleteAll()
            orgRateLimitConfigWriteRepository?.deleteAll()
            webClientConfigRepository?.deleteAll()

            // mydata_org_info는 테스트 데이터만 삭제(레퍼런스 데이터 보존)
            // 전체 엔티티 조회 후 테스트 데이터만 삭제
            orgInfoWriteRepository?.let { repo ->
                val allOrgInfos = repo.findAll().toList()
                val testOrgInfos = allOrgInfos.filter {
                    it.orgCode.startsWith("ORG_") || it.orgCode == "ORG_DELETE"
                }
                testOrgInfos.forEach { repo.delete(it) }
            }

            println("[DEBUG_LOG] Database tables cleaned up using repositories")
        } catch (e: Exception) {
            println("[DEBUG_LOG] Error cleaning up database tables: ${e.message}")
            // 테스트 실행이 중단되지 않도록 예외를 다시 던지지 않음
        }
    }

    private suspend fun cleanupRedisData() {
        try {
            // Redis 키 전체 삭제
            redissonReactiveClient?.let { client ->
                val keys = client.keys
                keys.flushall().awaitSingleOrNull()
                println("[DEBUG_LOG] Redis data cleaned up")
            }
        } catch (e: Exception) {
            println("[DEBUG_LOG] Error cleaning up Redis data: ${e.message}")
            // 테스트 실행이 중단되지 않도록 예외를 다시 던지지 않음
        }
    }


    companion object {
        private val redis = GenericContainer(DockerImageName.parse("redis:latest")).apply {
            withExposedPorts(6379)
            waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1).withStartupTimeout(Duration.ofSeconds(60)))
            withReuse(true) // 테스트 클래스 간 성능 향상을 위해 재사용 활성화
        }

        private val mysql = MySQLContainer(DockerImageName.parse("mysql:8.0.33")).apply {
            withDatabaseName("testdb")
            withUsername("myuser")
            withPassword("secret")
            waitingFor(Wait.forLogMessage(".*ready for connections.*", 1).withStartupTimeout(Duration.ofSeconds(90)))
            withReuse(true) // 테스트 클래스 간 성능 향상을 위해 재사용 활성화
        }

        private val mongo = MongoDBContainer(DockerImageName.parse("mongo:latest")).apply {
            waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
            withReuse(true) // 테스트 클래스 간 성능 향상을 위해 재사용 활성화
        }

        private val es = ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.10").apply {
            withEnv("xpack.security.enabled", "false")
            withEnv("discovery.type", "single-node")
            waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
            withReuse(true) // 테스트 클래스 간 성능 향상을 위해 재사용 활성화
        }

        private val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0")).apply {
            // Testcontainers용 단일 브로커 (bootstrapServers 제공)
            withReuse(true) // 테스트 클래스 간 성능 향상을 위해 재사용 활성화
            waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
        }

        private val dockerAvailable: Boolean = checkDockerAvailable()

        @Volatile private var started = false

        private fun checkDockerAvailable(): Boolean {
            return try {
                DockerClientFactory.instance().isDockerAvailable
            } catch (e: Throwable) {
                false
            }
        }

        private fun ensureStarted() {
            if (!dockerAvailable) return
            if (!started) synchronized(this) {
                if (!started) {
                    // 필수 컨테이너 먼저 시작
                    Startables.deepStart(listOf(redis, mysql)).join()
                    // 선택 컨테이너(있으면 사용)
                    try { mongo.start() } catch (_: Exception) {}
                    try { es.start() } catch (_: Exception) {}
                    try { kafka.start() } catch (_: Exception) {}
                    started = true
                }
            }
        }

        @JvmStatic @BeforeAll
        fun beforeAll() {
            Assumptions.assumeTrue(dockerAvailable, "Docker is not available. Skipping integration tests.")
            ensureStarted()
            if (started) {
                // JVM 종료 시 컨테이너가 중지되도록 종료 훅 등록
                Runtime.getRuntime().addShutdownHook(Thread {
                    stopContainers()
                })
            }
        }

        private fun stopContainers() {
            try {
                // 모든 테스트 컨테이너 중지
                if (redis.isRunning) {
                    redis.stop()
                    println("[DEBUG_LOG] Redis container stopped")
                }

                if (mysql.isRunning) {
                    mysql.stop()
                    println("[DEBUG_LOG] MySQL container stopped")
                }

                if (mongo.isRunning) {
                    mongo.stop()
                    println("[DEBUG_LOG] MongoDB container stopped")
                }

                if (kafka.isRunning) {
                    kafka.stop()
                    println("[DEBUG_LOG] Kafka container stopped")
                }

                if (es.isRunning) {
                    es.stop()
                    println("[DEBUG_LOG] Elasticsearch container stopped")
                }

                println("[DEBUG_LOG] All containers stopped successfully")
            } catch (e: Exception) {
                println("[DEBUG_LOG] Error stopping containers: ${e.message}")
                // 테스트 실행이 중단되지 않도록 예외를 다시 던지지 않음
            }
        }

        @JvmStatic
        @DynamicPropertySource
        fun props(reg: DynamicPropertyRegistry) {
            if (!dockerAvailable) return
            ensureStarted()
            if (!started) return

            // Redis
            reg.add("spring.redis.master") { "redis://${redis.host}:${redis.getMappedPort(6379)}" }
            reg.add("spring.redis.slave") { "redis://${redis.host}:${redis.getMappedPort(6379)}" }

            // Flyway(JDBC)
            reg.add("spring.flyway.url") {
                "jdbc:mysql://${mysql.host}:${mysql.getMappedPort(3306)}/${mysql.databaseName}?useUnicode=true&characterEncoding=utf8&connectionTimeZone=LOCAL"
            }
            reg.add("spring.flyway.user") { mysql.username }
            reg.add("spring.flyway.password") { mysql.password }

            // R2DBC
            val r2dbcUrl = "r2dbc:mysql://${mysql.host}:${mysql.getMappedPort(3306)}/${mysql.databaseName}"
            reg.add("spring.r2dbc.url") { r2dbcUrl }
            reg.add("spring.r2dbc.username") { mysql.username }
            reg.add("spring.r2dbc.password") { mysql.password }

            // 커스텀 master/slave 키도 동일하게 내려줌(있는 경우)
            reg.add("spring.r2dbc.master.url") { r2dbcUrl }
            reg.add("spring.r2dbc.master.username") { mysql.username }
            reg.add("spring.r2dbc.master.password") { mysql.password }
            reg.add("spring.r2dbc.slave.url") { r2dbcUrl }
            reg.add("spring.r2dbc.slave.username") { mysql.username }
            reg.add("spring.r2dbc.slave.password") { mysql.password }

            // Mongo (선택)
            if (mongo.isRunning) {
                reg.add("spring.data.mongodb.uri") { "mongodb://${mongo.host}:${mongo.getMappedPort(27017)}/testdb" }
            }

            // Elasticsearch (선택)
            if (es.isRunning) {
                reg.add("spring.elasticsearch.uris") { "http://${es.host}:${es.getMappedPort(9200)}" }
            }

            // Kafka (선택)
            if (kafka.isRunning) {
                reg.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            }
        }
    }
}

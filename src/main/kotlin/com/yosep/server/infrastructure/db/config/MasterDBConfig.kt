package com.finda.services.infra.db.config

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.core.R2dbcEntityOperations
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.dialect.MySqlDialect
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator

@Configuration
@EnableR2dbcRepositories(
    basePackages = ["com.yosep.server.infrastructure.db.common.write.repository"],
    entityOperationsRef = "masterEntityOperations",
)
class MasterDBConfig(
    @param:Value("\${spring.r2dbc.master.url}") private val url: String,
    @param:Value("\${spring.r2dbc.master.username}") private val username: String,
    @param:Value("\${spring.r2dbc.master.password}") private val password: String,
) {
    @Bean
    @Primary
    @Qualifier("master")
    fun masterConnectionFactory(): ConnectionFactory =
        ConnectionFactories.get(
            ConnectionFactoryOptions
                .parse(url)
                .mutate()
                .option(ConnectionFactoryOptions.USER, username)
                .option(ConnectionFactoryOptions.PASSWORD, password)
                .build(),
        )

    @Bean
    @Primary
    fun masterEntityOperations(
        @Qualifier("master") connectionFactory: ConnectionFactory,
    ): R2dbcEntityOperations {
        val strategy = DefaultReactiveDataAccessStrategy(MySqlDialect.INSTANCE)
        val client = DatabaseClient.builder().connectionFactory(connectionFactory).build()
        return R2dbcEntityTemplate(client, strategy)
    }

    @Bean
    @Primary
    fun masterTransactionManager(
        @Qualifier("master") connectionFactory: ConnectionFactory,
    ): ReactiveTransactionManager = R2dbcTransactionManager(connectionFactory)

    @Bean
    @Primary
    fun masterTransactionalOperator(
        @Qualifier("master") connectionFactory: ConnectionFactory,
    ): TransactionalOperator = TransactionalOperator.create(R2dbcTransactionManager(connectionFactory))
}

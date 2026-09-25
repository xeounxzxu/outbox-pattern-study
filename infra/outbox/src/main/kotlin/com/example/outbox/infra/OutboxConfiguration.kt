package com.example.outbox.infra

import one.tomorrow.transactionaloutbox.repository.OutboxLockRepository
import one.tomorrow.transactionaloutbox.repository.OutboxRepository
import one.tomorrow.transactionaloutbox.service.DefaultKafkaProducerFactory
import one.tomorrow.transactionaloutbox.service.OutboxProcessor
import one.tomorrow.transactionaloutbox.service.OutboxService
import one.tomorrow.transactionaloutbox.tracing.NoopTracingService
import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OutboxProperties::class)
class OutboxConfiguration {
    @Bean
    fun outboxRepository(jdbcTemplate: JdbcTemplate) = OutboxRepository(jdbcTemplate)

    @Bean
    fun outboxLockRepository(jdbcTemplate: JdbcTemplate, transactionManager: PlatformTransactionManager) =
        OutboxLockRepository(jdbcTemplate, transactionManager)

    @Bean
    fun outboxService(repository: OutboxRepository) = OutboxService(repository, NoopTracingService())

    @Bean
    @DependsOnDatabaseInitialization
    @ConditionalOnProperty(prefix = "outbox", name = ["processor-enabled"], havingValue = "true", matchIfMissing = true)
    fun outboxProcessor(
        repository: OutboxRepository,
        properties: OutboxProperties,
        beanFactory: AutowireCapableBeanFactory,
    ): OutboxProcessor {
        val producerProperties = properties.producer.mapValues { it.value as Any }.toMutableMap()
        producerProperties["enable.idempotence"] = true
        producerProperties["acks"] = "all"
        return OutboxProcessor(
            repository, DefaultKafkaProducerFactory(producerProperties),
            properties.processingInterval, properties.lockTimeout,
            properties.lockOwnerId, properties.eventSource, beanFactory,
        )
    }
}

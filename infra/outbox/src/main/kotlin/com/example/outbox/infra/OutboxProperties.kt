package com.example.outbox.infra

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.util.UUID

@ConfigurationProperties("outbox")
data class OutboxProperties(
    val processingInterval: Duration = Duration.ofMillis(200),
    val lockTimeout: Duration = Duration.ofSeconds(5),
    val lockOwnerId: String = UUID.randomUUID().toString(),
    val eventSource: String = "outbox-pattern",
    val producer: Map<String, String> = mapOf("bootstrap.servers" to "localhost:9092"),
) {
    init {
        require(!processingInterval.isNegative && processingInterval.toMillis() > 0)
        require(lockTimeout > processingInterval) { "Lock timeout must exceed processing interval" }
        require(lockOwnerId.isNotBlank() && lockOwnerId.length <= 128)
        require(eventSource.isNotBlank())
    }
}

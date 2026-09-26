package com.example.outbox.api

import one.tomorrow.transactionaloutbox.service.OutboxService
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper
import java.util.UUID

@Service
class OrderService(
    private val jdbcTemplate: JdbcTemplate,
    private val outboxService: OutboxService,
    private val objectMapper: ObjectMapper,
    @Value("\${orders.topic:orders.created}") private val topic: String,
) {
    @Transactional
    fun create(productName: String, quantity: Int): OrderCreated {
        val event = OrderCreated(UUID.randomUUID(), productName, quantity)
        jdbcTemplate.update(
            "insert into orders (id, product_name, quantity) values (?, ?, ?)",
            event.orderId.toString(), event.productName, event.quantity,
        )
        outboxService.saveForPublishing(
            topic, event.orderId.toString(), objectMapper.writeValueAsBytes(event),
            mapOf("event-type" to "OrderCreated", "content-type" to "application/json"),
        )
        return event
    }
}

data class OrderCreated(val orderId: UUID, val productName: String, val quantity: Int)

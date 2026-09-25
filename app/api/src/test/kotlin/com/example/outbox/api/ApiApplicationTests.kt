package com.example.outbox.api

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["outbox.processor-enabled=false"],
)
class ApiApplicationTests {
    companion object {
        class Postgres : GenericContainer<Postgres>("postgres:17-alpine")
        private val postgres = Postgres().apply {
            withEnv("POSTGRES_DB", "outbox")
            withEnv("POSTGRES_USER", "outbox")
            withEnv("POSTGRES_PASSWORD", "outbox")
            withExposedPorts(5432)
            waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\n", 2))
            start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { "jdbc:postgresql://${postgres.host}:${postgres.getMappedPort(5432)}/outbox" }
            registry.add("spring.datasource.username") { "outbox" }
            registry.add("spring.datasource.password") { "outbox" }
        }
    }

    @Value("\${local.server.port}")
    private var port: Int = 0
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var orders: OrderService
    @Autowired private lateinit var transactionManager: PlatformTransactionManager

    @Test
    fun `API commits order and pending event without Kafka`() {
        val before = count("outbox_kafka")
        val response = post("""{"productName":"Kotlin book","quantity":2}""")
        assertEquals(201, response.statusCode())
        assertTrue(response.body().contains("orderId"))
        assertEquals(before + 1, count("outbox_kafka"))
        assertEquals(1, jdbc.queryForObject(
            "select count(*) from outbox_kafka o join orders b on o.key = b.id::text where o.processed is null and b.product_name = ?",
            Int::class.java, "Kotlin book",
        ))
    }

    @Test
    fun `rollback cancels business data and outbox together`() {
        val ordersBefore = count("orders")
        val outboxBefore = count("outbox_kafka")
        assertFailsWith<IllegalStateException> {
            TransactionTemplate(transactionManager).executeWithoutResult {
                orders.create("rollback", 1)
                error("Simulated business failure after saving the event")
            }
        }
        assertEquals(ordersBefore, count("orders"))
        assertEquals(outboxBefore, count("outbox_kafka"))
    }

    @Test
    fun `invalid request cannot create order or event`() {
        val ordersBefore = count("orders")
        val outboxBefore = count("outbox_kafka")
        assertEquals(400, post("""{"productName":"","quantity":0}""").statusCode())
        assertEquals(ordersBefore, count("orders"))
        assertEquals(outboxBefore, count("outbox_kafka"))
    }

    private fun count(table: String): Int = jdbc.queryForObject("select count(*) from $table", Int::class.java)!!
    private fun post(body: String): HttpResponse<String> = HttpClient.newHttpClient().send(
        HttpRequest.newBuilder(URI("http://localhost:$port/api/orders"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString(),
    )
}

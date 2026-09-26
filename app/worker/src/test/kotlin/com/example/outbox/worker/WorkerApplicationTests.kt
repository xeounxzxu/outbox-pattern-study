package com.example.outbox.worker

import com.example.outbox.api.ApiApplication
import one.tomorrow.transactionaloutbox.service.OutboxProcessor
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.mysql.MySQLContainer
import org.testcontainers.kafka.KafkaContainer
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WorkerApplicationTests {

    @Test
    fun `API publishes and worker takes over pending events after Kafka outage`() {
        MySQLContainer("mysql:8.4").apply {
            withDatabaseName("outbox")
            withUsername("outbox")
            withPassword("outbox")
            withUrlParam("connectionTimeZone", "UTC")
            withUrlParam("forceConnectionTimeZoneToSession", "true")
            start()
        }.use { mysql ->
            KafkaContainer("apache/kafka:4.1.1").apply { start() }.use { kafka ->
                AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use {
                    it.createTopics(listOf(NewTopic("orders.created", 1, 1.toShort())))
                        .all().get(30, TimeUnit.SECONDS)
                }
                val common = arrayOf(
                    "--spring.datasource.url=${mysql.jdbcUrl}",
                    "--spring.datasource.username=outbox", "--spring.datasource.password=outbox",
                    "--spring.config.import=classpath:outbox.yml",
                    "--outbox.producer[bootstrap.servers]=${kafka.bootstrapServers}",
                    "--outbox.producer[delivery.timeout.ms]=2000",
                    "--outbox.producer[request.timeout.ms]=1000",
                    "--outbox.producer[max.block.ms]=1000",
                    "--outbox.lock-timeout=2s", "--server.port=0",
                )
                val api = SpringApplicationBuilder(ApiApplication::class.java)
                    .run(*common, "--spring.application.name=test-api", "--outbox.lock-owner-id=test-api")
                try {
                    assertTrue(api.getBean(OutboxProcessor::class.java).isActive)
                    SpringApplicationBuilder(WorkerApplication::class.java)
                        .run(*common, "--spring.application.name=test-worker", "--outbox.lock-owner-id=test-worker").use { worker ->
                            assertFalse(worker.getBean(OutboxProcessor::class.java).isActive)
                            val jdbc = worker.getBean(JdbcTemplate::class.java)
                            val consumerProperties = mapOf<String, Any>(
                                "bootstrap.servers" to kafka.bootstrapServers,
                                "group.id" to UUID.randomUUID().toString(),
                                "auto.offset.reset" to "earliest",
                                "enable.auto.commit" to false,
                                "key.deserializer" to StringDeserializer::class.java,
                                "value.deserializer" to ByteArrayDeserializer::class.java,
                            )
                            KafkaConsumer<String, ByteArray>(consumerProperties).use { consumer ->
                                consumer.subscribe(listOf("orders.created"))
                                postOrder(api, "first")
                                await().atMost(Duration.ofSeconds(30)).untilAsserted {
                                    assertEquals(1, jdbc.queryForObject("select count(*) from outbox_kafka where processed is not null", Int::class.java))
                                }
                                val first = receive(consumer, "first")
                                assertEquals("outbox-pattern", String(first.headers().lastHeader("x-source").value()))
                                assertTrue(first.headers().lastHeader("x-sequence").value().isNotEmpty())

                                kafka.dockerClient.pauseContainerCmd(kafka.containerId).exec()
                                try {
                                    postOrder(api, "recover")
                                    // Longer than producer delivery timeout: a failed send must remain pending.
                                    await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted {
                                        assertEquals(1, jdbc.queryForObject("select count(*) from outbox_kafka where processed is null", Int::class.java))
                                    }
                                    api.close()
                                } finally {
                                    kafka.dockerClient.unpauseContainerCmd(kafka.containerId).exec()
                                }
                                await().atMost(Duration.ofSeconds(40)).untilAsserted {
                                    assertTrue(worker.getBean(OutboxProcessor::class.java).isActive)
                                    assertEquals(0, jdbc.queryForObject("select count(*) from outbox_kafka where processed is null", Int::class.java))
                                    assertEquals(2, jdbc.queryForObject("select count(*) from outbox_kafka", Int::class.java))
                                }
                                val recovered = receive(consumer, "recover")
                                assertEquals("outbox-pattern", String(recovered.headers().lastHeader("x-source").value()))
                            }
                            val health = HttpClient.newHttpClient().send(
                                HttpRequest.newBuilder(URI("http://localhost:${worker.environment.getProperty("local.server.port")}/actuator/health")).build(),
                                HttpResponse.BodyHandlers.ofString(),
                            )
                            assertEquals(200, health.statusCode())
                        }
                } finally {
                    api.close()
                }
            }
        }
    }

    private fun postOrder(context: ConfigurableApplicationContext, name: String) {
        val port = context.environment.getProperty("local.server.port")
        val response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI("http://localhost:$port/api/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"productName":"$name","quantity":1}""")).build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        assertEquals(201, response.statusCode(), response.body())
    }

    private fun receive(consumer: KafkaConsumer<String, ByteArray>, name: String): org.apache.kafka.clients.consumer.ConsumerRecord<String, ByteArray> {
        var found: org.apache.kafka.clients.consumer.ConsumerRecord<String, ByteArray>? = null
        await().atMost(Duration.ofSeconds(30)).until {
            found = consumer.poll(Duration.ofMillis(300)).firstOrNull { String(it.value()).contains(name) }
            found != null
        }
        return requireNotNull(found)
    }
}

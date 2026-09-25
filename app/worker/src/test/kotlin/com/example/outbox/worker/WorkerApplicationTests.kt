package com.example.outbox.worker

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.scheduling.config.ScheduledTaskHolder
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkerApplicationTests {
    @Value("\${local.server.port}")
    private var port: Int = 0

    @Autowired
    private lateinit var context: ApplicationContext

    @Test
    fun `worker is healthy and polling job is registered`() {
        val request = HttpRequest.newBuilder(URI("http://localhost:$port/actuator/health")).GET().build()
        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("UP"))
        val tasks = context.getBeansOfType(ScheduledTaskHolder::class.java).values.flatMap { it.scheduledTasks }
        assertEquals(1, tasks.size)
    }
}

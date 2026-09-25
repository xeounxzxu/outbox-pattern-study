package com.example.outbox.api

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiApplicationTests {
    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun `API responds over HTTP`() {
        val request = HttpRequest.newBuilder(URI("http://localhost:$port/api/hello")).GET().build()
        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("Hello from API server"))
    }
}

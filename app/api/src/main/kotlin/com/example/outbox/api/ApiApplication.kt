package com.example.outbox.api

import com.example.outbox.infra.OutboxConfiguration
import org.springframework.context.annotation.Import
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
@Import(OutboxConfiguration::class)
class ApiApplication

fun main(args: Array<String>) {
    runApplication<ApiApplication>(*args)
}

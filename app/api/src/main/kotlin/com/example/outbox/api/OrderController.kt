package com.example.outbox.api

import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
class OrderController(private val orderService: OrderService) {
    @PostMapping("/api/orders")
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@Valid @RequestBody request: CreateOrderRequest): OrderCreated =
        orderService.create(request.productName, request.quantity)
}

data class CreateOrderRequest(
    @field:NotBlank @field:Size(max = 200) val productName: String,
    @field:Min(1) val quantity: Int,
)

package com.leopay.bookingapi.gateway

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
open class PgTimeoutResolver(
    @Value("\${app.load-level:normal}") loadLevel: String,
) {
    open val seconds: Long = when {
        loadLevel.isBlank() -> 10L
        loadLevel.lowercase() == "peak" -> 20L
        else -> 10L
    }.also { require(it > 0) { "PG timeout must be greater than 0, but got: $it" } }
}

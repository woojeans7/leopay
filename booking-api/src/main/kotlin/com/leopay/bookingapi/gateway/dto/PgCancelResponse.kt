package com.leopay.bookingapi.gateway.dto

import java.time.LocalDateTime

data class PgCancelResponse(
    val canceledAt: LocalDateTime,
)

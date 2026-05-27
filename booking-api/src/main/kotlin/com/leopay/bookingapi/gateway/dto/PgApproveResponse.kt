package com.leopay.bookingapi.gateway.dto

import java.time.LocalDateTime

data class PgApproveResponse(
    val pgTransactionId: String,
    val approvedAt: LocalDateTime,
)

package com.leopay.bookingapi.gateway.dto

data class PgCancelRequest(
    val pgTransactionId: String,
    val cancelReason: String,
)

package com.leopay.mockpg.dto

data class PgCancelRequest(
    val pgTransactionId: String,
    val cancelReason: String,
)
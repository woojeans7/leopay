package com.leopay.mockpg.dto

import java.time.LocalDateTime

data class PgApproveResponse(
    val pgTransactionId: String,
    val approvedAt: LocalDateTime,
)
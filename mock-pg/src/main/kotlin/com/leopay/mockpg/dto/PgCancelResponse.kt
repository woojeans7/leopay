package com.leopay.mockpg.dto

import java.time.LocalDateTime

data class PgCancelResponse(
    val canceledAt: LocalDateTime,
)
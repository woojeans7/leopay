package com.leopay.bookingapi.gateway.dto

import java.math.BigDecimal

data class PgApproveRequest(
    val paymentKey: String,
    val amount: BigDecimal,
    val method: String,
)

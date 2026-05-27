package com.leopay.bookingapi.dto

import com.leopay.core.enums.PaymentStatus

data class PaymentCreateResponse(
    val paymentId: Long,
    val paymentKey: String,
    val status: PaymentStatus,
)

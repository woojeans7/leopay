package com.leopay.bookingapi.dto

import com.leopay.core.enums.PaymentMethod
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Positive
import java.math.BigDecimal

data class PaymentCreateRequest(
    @field:NotBlank
    val paymentKey: String,

    @field:NotNull
    val merchantId: Long,

    @field:NotNull
    @field:Positive
    val amount: BigDecimal,

    @field:NotNull
    val method: PaymentMethod,
)

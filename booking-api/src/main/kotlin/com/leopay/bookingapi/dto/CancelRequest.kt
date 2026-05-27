package com.leopay.bookingapi.dto

import jakarta.validation.constraints.NotBlank

data class CancelRequest(
    @field:NotBlank
    val cancelReason: String,
)

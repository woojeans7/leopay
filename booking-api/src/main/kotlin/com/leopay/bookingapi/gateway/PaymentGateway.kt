package com.leopay.bookingapi.gateway

import com.leopay.bookingapi.gateway.dto.PgApproveRequest
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.gateway.dto.PgCancelRequest
import com.leopay.bookingapi.gateway.dto.PgCancelResponse

interface PaymentGateway {
    suspend fun approve(request: PgApproveRequest): PgApproveResponse
    suspend fun cancel(request: PgCancelRequest): PgCancelResponse
}

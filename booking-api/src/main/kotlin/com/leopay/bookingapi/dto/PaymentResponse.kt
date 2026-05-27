package com.leopay.bookingapi.dto

import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import java.math.BigDecimal
import java.time.LocalDateTime

data class PaymentResponse(
    val id: Long,
    val paymentKey: String,
    val merchantId: Long,
    val userId: String,
    val amount: BigDecimal,
    val method: PaymentMethod,
    val status: PaymentStatus,
    val pgTransactionId: String?,
    val approvedAt: LocalDateTime?,
    val canceledAt: LocalDateTime?,
) {
    companion object {
        fun from(entity: PaymentEntity): PaymentResponse = PaymentResponse(
            id = entity.id!!,
            paymentKey = entity.paymentKey,
            merchantId = entity.merchantId,
            userId = entity.userId,
            amount = entity.amount,
            method = entity.method,
            status = entity.status,
            pgTransactionId = entity.pgTransactionId,
            approvedAt = entity.approvedAt,
            canceledAt = entity.canceledAt,
        )
    }
}

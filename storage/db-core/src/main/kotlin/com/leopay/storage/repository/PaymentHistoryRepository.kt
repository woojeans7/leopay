package com.leopay.storage.repository

import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentHistoryEntity
import org.springframework.data.jpa.repository.JpaRepository

interface PaymentHistoryRepository : JpaRepository<PaymentHistoryEntity, Long> {

    fun findByPaymentIdOrderByCreatedAtAsc(paymentId: Long): List<PaymentHistoryEntity>

    fun findByPaymentIdAndNewStatus(paymentId: Long, newStatus: PaymentStatus): List<PaymentHistoryEntity>
}

package com.leopay.storage.repository

import com.leopay.storage.entity.PaymentHistoryEntity
import org.springframework.data.jpa.repository.JpaRepository

interface PaymentHistoryRepository : JpaRepository<PaymentHistoryEntity, Long> {

    fun findByPaymentIdOrderByCreatedAtAsc(paymentId: Long): List<PaymentHistoryEntity>
}

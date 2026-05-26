package com.leopay.storage.repository

import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import java.util.Optional

interface PaymentRepository : JpaRepository<PaymentEntity, Long> {

    fun findByPaymentKey(paymentKey: String): Optional<PaymentEntity>

    fun findByMerchantIdAndStatus(merchantId: Long, status: PaymentStatus, pageable: Pageable): List<PaymentEntity>

    fun existsByPaymentKey(paymentKey: String): Boolean
}

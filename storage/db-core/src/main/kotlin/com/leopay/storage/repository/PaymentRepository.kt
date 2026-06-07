package com.leopay.storage.repository

import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDate
import java.util.Optional

interface PaymentRepository : JpaRepository<PaymentEntity, Long> {

    fun findByPaymentKey(paymentKey: String): Optional<PaymentEntity>

    fun findByMerchantIdAndStatus(merchantId: Long, status: PaymentStatus, pageable: Pageable): List<PaymentEntity>

    fun existsByPaymentKey(paymentKey: String): Boolean

    /**
     * 특정 날짜에 APPROVED 결제가 존재하는 merchantId 목록 조회.
     *
     * 정산 스케줄러(SettlementScheduler)에서 전날 정산 대상 가맹점을 파악하기 위해 사용.
     * DISTINCT를 사용하여 merchantId 중복 없이 반환.
     *
     * 인덱스: idx_payment_merchant_approved (merchant_id, approved_at) 활용.
     */
    @Query(
        """
        SELECT DISTINCT p.merchantId FROM PaymentEntity p
        WHERE p.status = :status
          AND FUNCTION('DATE', p.approvedAt) = :settlementDate
        """
    )
    fun findDistinctMerchantIdsByStatusAndApprovedDate(
        @Param("status") status: PaymentStatus,
        @Param("settlementDate") settlementDate: LocalDate,
    ): List<Long>
}

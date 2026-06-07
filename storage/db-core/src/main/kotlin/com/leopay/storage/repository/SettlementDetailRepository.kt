package com.leopay.storage.repository

import com.leopay.core.enums.SettlementStatus
import com.leopay.storage.entity.SettlementDetailEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDate

interface SettlementDetailRepository : JpaRepository<SettlementDetailEntity, Long> {

    fun findBySettlementId(settlementId: Long): List<SettlementDetailEntity>

    // 배치 집계 시 사용 — 특정 가맹점·기준일의 PENDING 상태 건만 읽기
    fun findByMerchantIdAndSettlementDateAndStatus(
        merchantId: Long,
        settlementDate: LocalDate,
        status: SettlementStatus,
    ): List<SettlementDetailEntity>

    // Consumer 중복 적재 방지 — 동일 payment_id 존재 여부 확인
    fun existsByPaymentId(paymentId: Long): Boolean

    // 배치 스케줄러용 — 특정 기준일에 PENDING 상태인 가맹점 목록 조회
    @Query(
        """
        SELECT DISTINCT sd.merchantId FROM SettlementDetailEntity sd
        WHERE sd.settlementDate = :settlementDate
          AND sd.status = :status
        """
    )
    fun findDistinctMerchantIdsBySettlementDateAndStatus(
        @Param("settlementDate") settlementDate: LocalDate,
        @Param("status") status: SettlementStatus,
    ): List<Long>
}

package com.leopay.settlementworker.batch.dto

import java.math.BigDecimal
import java.time.LocalDate

/**
 * 정산 배치 Processor → Writer 간 데이터 전달 객체.
 * B-5: 모든 금액 필드는 BigDecimal 타입 사용 (부동소수점 오차 방지)
 */
data class SettlementItemDto(
    val settlementDetailId: Long,   // Writer에서 settlement_detail.settlement_id, status 업데이트 시 사용
    val paymentId: Long,
    val merchantId: Long,
    val feeRate: BigDecimal,
    val amount: BigDecimal,
    val feeAmount: BigDecimal,
    val settlementAmount: BigDecimal,
    val settlementDate: LocalDate,
)

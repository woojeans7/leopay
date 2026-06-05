package com.leopay.settlementworker.batch.writer

import com.leopay.core.enums.SettlementStatus
import com.leopay.settlementworker.batch.dto.SettlementItemDto
import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.entity.SettlementEntity
import com.leopay.storage.repository.SettlementDetailRepository
import com.leopay.storage.repository.SettlementRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component
import java.math.BigDecimal

/**
 * 정산 배치 ItemWriter.
 *
 * 청크 단위로:
 * 1. SettlementEntity upsert: merchantId + settlementDate unique 제약 활용
 *    - 이미 존재하면 totalCount/totalAmount/feeAmount/settlementAmount 누적
 *    - 없으면 신규 생성 (status = PENDING)
 * 2. SettlementDetailEntity bulk insert: 각 paymentId 별로 상세 내역 저장
 *
 * B-4: 청크 단위 트랜잭션 보장
 *   - SettlementJobConfig의 chunk() 설정에 의해 이 Writer 호출 전체가 하나의 트랜잭션으로 묶임.
 *   - Writer 내 예외 발생 시 해당 청크 전체가 롤백되며, Skip 정책에 따라 재처리됨.
 *
 * B-3: 중복 집계 방지
 *   - SettlementDetailEntity의 paymentId 중복 삽입은 DB unique 제약(settlement_detail.payment_id)으로 차단 가능.
 *   - 단, settlement_detail 테이블에 payment_id unique 제약이 없는 경우 existsBySettlementIdAndPaymentId 확인으로 보완.
 */
@Component
class SettlementWriter(
    private val settlementRepository: SettlementRepository,
    private val settlementDetailRepository: SettlementDetailRepository,
) : ItemWriter<SettlementItemDto> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun write(chunk: Chunk<out SettlementItemDto>) {
        if (chunk.isEmpty) return

        // 청크 내 아이템은 모두 동일 merchantId + settlementDate (Step 파라미터로 고정)
        val first = chunk.items.first()

        // 1. SettlementEntity upsert
        val settlement = settlementRepository
            .findByMerchantIdAndSettlementDate(first.merchantId, first.settlementDate)
            .orElseGet {
                // 신규 생성 — 첫 청크에서 한 번만 INSERT
                SettlementEntity(
                    merchantId = first.merchantId,
                    settlementDate = first.settlementDate,
                    totalCount = 0,
                    totalAmount = BigDecimal.ZERO,
                    feeAmount = BigDecimal.ZERO,
                    settlementAmount = BigDecimal.ZERO,
                    appliedFeeRate = first.feeRate,
                    status = SettlementStatus.PENDING,
                )
            }

        // 청크 내 모든 아이템을 정산 집계에 누적
        var accumulatedCount = 0
        var accumulatedTotal = BigDecimal.ZERO
        var accumulatedFee = BigDecimal.ZERO
        var accumulatedSettlement = BigDecimal.ZERO

        for (item in chunk.items) {
            accumulatedCount++
            accumulatedTotal = accumulatedTotal.add(item.amount)
            accumulatedFee = accumulatedFee.add(item.feeAmount)
            accumulatedSettlement = accumulatedSettlement.add(item.settlementAmount)
        }

        settlement.totalCount += accumulatedCount
        settlement.totalAmount = settlement.totalAmount.add(accumulatedTotal)
        settlement.feeAmount = settlement.feeAmount.add(accumulatedFee)
        settlement.settlementAmount = settlement.settlementAmount.add(accumulatedSettlement)

        val savedSettlement = settlementRepository.save(settlement)

        log.debug(
            "정산 누적: settlementId={}, merchantId={}, +{}건, +{}원",
            savedSettlement.id, savedSettlement.merchantId, accumulatedCount, accumulatedTotal
        )

        // 2. SettlementDetailEntity bulk insert
        // B-3: 이미 처리된 paymentId에 대한 중복 삽입을 방지하기 위해 기존 상세 내역 paymentId를 조회
        val existingPaymentIds = savedSettlement.id?.let { sid ->
            settlementDetailRepository.findBySettlementId(sid).map { it.paymentId }.toSet()
        } ?: emptySet()

        val details = chunk.items
            .filter { it.paymentId !in existingPaymentIds }
            .map { item ->
                SettlementDetailEntity(
                    settlementId = savedSettlement.id!!,
                    paymentId = item.paymentId,
                    amount = item.amount,
                    feeAmount = item.feeAmount,
                )
            }

        if (details.isNotEmpty()) {
            settlementDetailRepository.saveAll(details)
            log.debug("정산 상세 {}건 저장: settlementId={}", details.size, savedSettlement.id)
        }
    }
}

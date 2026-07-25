package com.leopay.settlementworker.batch.processor

import com.leopay.settlementworker.batch.dto.SettlementItemDto
import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.repository.MerchantRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.RoundingMode

/**
 * 정산 배치 ItemProcessor.
 *
 * SettlementDetailEntity → SettlementItemDto 변환.
 *
 * Consumer가 PAYMENT_APPROVED 수신 시 feeAmount를 이미 계산하여 settlement_detail에 저장했으므로
 * 수수료 재계산은 불필요하다. MerchantRepository에서 feeRate만 조회하여 settlementAmount를 산출한다.
 *
 * B-5: 수수료 부동소수점 오차 방지 — 반올림 정책은 계산 단계별로 다르다
 *   - Double/Float 사용 시 3.5% 수수료가 0.034999... 또는 0.035000000000001 로 표현될 수 있음 → 전 구간 BigDecimal 사용
 *   - feeAmount (SettlementDetailService, scale=4, 중간 계산값): HALF_EVEN(Banker's Rounding) — 통계적 편향 없는 반올림
 *   - settlementAmount (이 클래스, scale=0, 최종 송금액): FLOOR(절사) — 금융 정산 관행상 PG사가 원 단위 미만을
 *     반올림하여 과지급하지 않도록 보장 (커밋 4c02b99에서 HALF_EVEN → FLOOR로 의도적으로 변경됨)
 */
@Component
@StepScope
class SettlementProcessor(
    private val merchantRepository: MerchantRepository,
    @Value("#{jobParameters['settlementDate']}") private val settlementDateStr: String,
) : ItemProcessor<SettlementDetailEntity, SettlementItemDto> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun process(sd: SettlementDetailEntity): SettlementItemDto? {
        val merchant = merchantRepository.findById(sd.merchantId).orElse(null)
        if (merchant == null) {
            // Skip 처리: 가맹점 정보 없음 → 로그 후 null 반환 (Spring Batch는 null 반환 시 해당 아이템 skip)
            log.warn("가맹점 정보 없음: merchantId={}, settlementDetailId={} — skip", sd.merchantId, sd.id)
            return null
        }

        // B-5: feeAmount는 Consumer가 이미 계산하여 settlement_detail에 저장 — 재계산 불필요
        // settlementAmount = amount - feeAmount, 원 단위(scale=0) FLOOR(버림)
        // 금융 정산 관행: 원 단위 미만은 절사 — PG사가 초과 지급하지 않도록 보장
        val settlementAmount = sd.amount
            .subtract(sd.feeAmount)
            .setScale(0, RoundingMode.FLOOR)

        log.debug(
            "정산 상세 처리: settlementDetailId={}, paymentId={}, amount={}, feeRate={}, feeAmount={}, settlementAmount={}",
            sd.id, sd.paymentId, sd.amount, merchant.feeRate, sd.feeAmount, settlementAmount
        )

        return SettlementItemDto(
            settlementDetailId = sd.id!!,
            paymentId = sd.paymentId,
            merchantId = sd.merchantId,
            feeRate = merchant.feeRate,
            amount = sd.amount,
            feeAmount = sd.feeAmount,
            settlementAmount = settlementAmount,
            settlementDate = sd.settlementDate,
        )
    }
}

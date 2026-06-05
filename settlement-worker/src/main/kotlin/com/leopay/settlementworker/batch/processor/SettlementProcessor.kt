package com.leopay.settlementworker.batch.processor

import com.leopay.settlementworker.batch.dto.SettlementItemDto
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.MerchantRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate

/**
 * 정산 배치 ItemProcessor.
 *
 * PaymentEntity → SettlementItemDto 변환.
 * MerchantRepository에서 feeRate를 조회한 뒤 수수료 및 정산 금액을 계산한다.
 *
 * B-5: 수수료 부동소수점 오차 방지
 *   - Double/Float 사용 시 3.5% 수수료가 0.034999... 또는 0.035000000000001 로 표현될 수 있음
 *   - BigDecimal + HALF_EVEN(Banker's Rounding) 으로 통계적 편향 없는 반올림 보장
 *   - feeAmount : scale=4 (소수점 4자리) — 중간 계산값 정밀도 확보
 *   - settlementAmount: scale=0 (원 단위 정수) — 최종 송금액은 원 단위 절사
 */
@Component
@StepScope
class SettlementProcessor(
    private val merchantRepository: MerchantRepository,
    @Value("#{jobParameters['settlementDate']}") private val settlementDateStr: String,
) : ItemProcessor<PaymentEntity, SettlementItemDto> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun process(payment: PaymentEntity): SettlementItemDto? {
        val merchant = merchantRepository.findById(payment.merchantId).orElse(null)
        if (merchant == null) {
            // Skip 처리: 가맹점 정보 없음 → 로그 후 null 반환 (Spring Batch는 null 반환 시 해당 아이템 skip)
            log.warn("가맹점 정보 없음: merchantId={}, paymentId={} — skip", payment.merchantId, payment.id)
            return null
        }

        // B-5: BigDecimal + HALF_EVEN 반올림으로 수수료 오차 방지
        // feeAmount = amount × feeRate, scale=4 자리로 유지 (소수점 이하 정밀도)
        val feeAmount = payment.amount
            .multiply(merchant.feeRate)
            .setScale(4, RoundingMode.HALF_EVEN)

        // settlementAmount = amount - feeAmount, 원 단위(scale=0) 반올림
        val settlementAmount = payment.amount
            .subtract(feeAmount)
            .setScale(0, RoundingMode.HALF_EVEN)

        log.debug(
            "결제 처리: paymentId={}, amount={}, feeRate={}, feeAmount={}, settlementAmount={}",
            payment.id, payment.amount, merchant.feeRate, feeAmount, settlementAmount
        )

        return SettlementItemDto(
            paymentId = payment.id!!,
            merchantId = payment.merchantId,
            feeRate = merchant.feeRate,
            amount = payment.amount,
            feeAmount = feeAmount,
            settlementAmount = settlementAmount,
            settlementDate = LocalDate.parse(settlementDateStr),
        )
    }
}

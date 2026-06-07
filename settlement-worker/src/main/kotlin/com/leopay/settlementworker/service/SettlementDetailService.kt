package com.leopay.settlementworker.service

import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.RoundingMode

@Service
class SettlementDetailService(
    private val paymentRepository: PaymentRepository,
    private val merchantRepository: MerchantRepository,
    private val settlementDetailRepository: SettlementDetailRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * settlement_detail 적재 공통 로직
     *
     * B-3: existsByPaymentId 로 중복 체크 — 이미 적재된 paymentId 는 skip
     * B-5: feeAmount = amount × feeRate, setScale(4, HALF_EVEN) — 부동소수점 오차 방지
     *
     * @param paymentId 적재할 결제 ID
     * @param source    호출 출처 구분 ("consumer" or "dlt") — 로그 식별 용도
     */
    @Transactional
    fun saveSettlementDetail(paymentId: Long, source: String = "consumer") {
        // B-3: 중복 소비 방지 — 이미 적재된 paymentId 는 skip
        if (settlementDetailRepository.existsByPaymentId(paymentId)) {
            log.warn("[settlement] 중복 이벤트 skip paymentId={} source={}", paymentId, source)
            return
        }

        val payment = paymentRepository.findById(paymentId).orElseThrow {
            IllegalStateException("PaymentEntity 조회 실패 paymentId=$paymentId")
        }

        val merchant = merchantRepository.findById(payment.merchantId).orElseThrow {
            IllegalStateException("MerchantEntity 조회 실패 merchantId=${payment.merchantId}")
        }

        val approvedAt = requireNotNull(payment.approvedAt) {
            "approvedAt 이 null 입니다 paymentId=$paymentId"
        }

        // B-5: BigDecimal + HALF_EVEN 으로 수수료 계산 (부동소수점 오차 방지)
        val feeAmount = payment.amount
            .multiply(merchant.feeRate)
            .setScale(4, RoundingMode.HALF_EVEN)

        val detail = SettlementDetailEntity(
            settlementId = null,
            paymentId = paymentId,
            merchantId = payment.merchantId,
            settlementDate = approvedAt.toLocalDate(),
            amount = payment.amount,
            feeAmount = feeAmount,
        )

        settlementDetailRepository.save(detail)

        log.info(
            "[settlement] settlement_detail 적재 paymentId={} merchantId={} source={}",
            paymentId,
            payment.merchantId,
            source,
        )
    }
}

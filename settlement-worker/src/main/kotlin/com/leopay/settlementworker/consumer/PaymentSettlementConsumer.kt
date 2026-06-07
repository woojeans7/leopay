package com.leopay.settlementworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.settlementworker.dto.PaymentEvent
import com.leopay.settlementworker.service.SettlementDetailService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 2단계(해결): 에러 핸들러 + DLT 가 적용된 정산 컨슈머
 *
 * settlementListenerContainerFactory 를 통해 DefaultErrorHandler 가 적용된다.
 *   - 1초 간격 최대 3회 재시도
 *   - 재시도 소진 시 payment.approved.DLT 로 이동 → SettlementDltConsumer 가 재처리
 *
 * A-4 1단계(문제 재현)는 테스트에서 @TestConfiguration 으로
 * 에러 핸들러 없는 팩토리를 override 하여 메시지 유실 상황을 재현한다.
 *
 * 정산 흐름:
 *   1. Consumer: payment.approved 수신 → settlement_detail 선적재 (PENDING)
 *   2. Batch: settlement_detail 기준으로 집계 → settlement 생성
 */
@Component
class PaymentSettlementConsumer(
    private val objectMapper: ObjectMapper,
    private val settlementDetailService: SettlementDetailService,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * payment.approved 토픽 컨슈머
     *
     * payment.approved 이벤트 수신 시 SettlementDetailService 에 위임하여
     * settlement_detail 테이블에 선적재한다.
     * 이후 배치가 settlement_detail 기준으로 집계하여 정산을 완료한다.
     *
     * B-3/B-5 처리는 SettlementDetailService 내부에서 수행한다.
     * DataIntegrityViolationException(unique constraint 위반) 은 중복 소비이므로
     * consumer 레벨(트랜잭션 경계 바깥)에서 catch 하여 DLT 미발동 처리한다.
     */
    @KafkaListener(
        topics = ["payment.approved"],
        groupId = "settlement-consumer-group",
        containerFactory = "settlementListenerContainerFactory",
    )
    fun onPaymentApproved(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)
        try {
            settlementDetailService.saveSettlementDetail(event.paymentId, source = "consumer")
        } catch (e: DataIntegrityViolationException) {
            // 동시 중복 소비 — unique constraint 위반은 정상 idempotency skip → DLT 미발동
            log.warn("[idempotency] 중복 소비 skip paymentId={}", event.paymentId)
        }
    }
}

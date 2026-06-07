package com.leopay.settlementworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.settlementworker.dto.PaymentEvent
import com.leopay.settlementworker.service.SettlementDetailService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 2단계(해결): DLT 컨슈머 — 재시도 3회 소진 후 DLT 로 이동된 메시지 처리
 *
 * DLT 이동 조건:
 *   - KafkaConsumerConfig 의 settlementListenerContainerFactory (에러 핸들러 포함) 가
 *     적용된 PaymentSettlementConsumer 에서 1초 간격 3회 재시도를 모두 소진한 메시지
 *   - 비즈니스 예외/DB 장애/배치 실패 등 모든 RuntimeException 대상
 *
 * 처리 방침:
 *   - paymentId 및 실패 원인을 WARN 레벨 로그로 기록
 *   - DLT 이동 시점의 실패 원인(헤더)을 추출하여 모니터링/알림 연동 가능
 *   - settlement_detail 에 PENDING 으로 재적재 → 배치가 정상 집계 가능
 *     (B-3 멱등성 보장: 이미 적재된 paymentId 는 Service 내부에서 skip)
 *   - 수동 재처리가 필요한 경우 DLT 메시지를 원본 토픽으로 재발행
 *
 * settlementDltListenerContainerFactory: 에러 핸들러 없는 단순 팩토리
 *   DLT Consumer 에 에러 핸들러를 달면 실패 시 또 DLT 로 이동하는 순환 구조가 되므로
 *   의도적으로 제거 — DLT Consumer 실패는 수동 운영 대응
 */
@Component
class SettlementDltConsumer(
    private val objectMapper: ObjectMapper,
    private val settlementDetailService: SettlementDetailService,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["payment.approved.DLT"],
        groupId = "settlement-dlt-group",
        containerFactory = "settlementDltListenerContainerFactory",
    )
    fun onPaymentApprovedDlt(record: ConsumerRecord<String, String>) {
        val failureReason = extractFailureReason(record)
        val event = parseEvent(record)

        log.warn(
            "[dlt] payment.approved.DLT 수신 — 정산 처리 최종 실패 paymentId={} reason={}",
            event?.paymentId, failureReason
        )

        // A-4: DLT 수신 시 settlement_detail 에 PENDING 으로 재적재
        // 정상 컨슈머에서 실패한 메시지도 배치가 집계할 수 있도록 보장
        if (event != null) {
            settlementDetailService.saveSettlementDetail(event.paymentId, source = "dlt")
        } else {
            log.error("[dlt] 이벤트 파싱 실패로 재적재 불가 — 수동 처리 필요 value={}", record.value())
        }
    }

    private fun parseEvent(record: ConsumerRecord<String, String>): PaymentEvent? =
        runCatching {
            objectMapper.readValue(record.value(), PaymentEvent::class.java)
        }.getOrElse { ex ->
            log.error("[dlt] 메시지 파싱 실패 value={} error={}", record.value(), ex.message)
            null
        }

    /** DLT 헤더에서 Spring Kafka 가 자동으로 첨부하는 원인 메시지를 추출한다. */
    private fun extractFailureReason(record: ConsumerRecord<String, String>): String {
        val header = record.headers().lastHeader("kafka_dlt-exception-message")
        return header?.value()?.let { String(it) } ?: "unknown"
    }
}

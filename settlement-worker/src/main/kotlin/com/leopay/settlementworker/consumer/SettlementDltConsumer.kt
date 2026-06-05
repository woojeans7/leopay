package com.leopay.settlementworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.settlementworker.dto.PaymentEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 2단계(해결): DLT 컨슈머 — 재시도 3회 소진 후 DLT 로 이동된 메시지 처리
 *
 * DLT 이동 조건:
 *   - KafkaConsumerConfig 의 settlementDltListenerContainerFactory 가 적용된 컨슈머에서
 *     1초 간격 3회 재시도를 모두 소진한 메시지
 *   - 비즈니스 예외/DB 장애/배치 실패 등 모든 RuntimeException 대상
 *
 * 처리 방침:
 *   - paymentId 및 실패 원인을 WARN 레벨 로그로 기록
 *   - DLT 이동 시점의 실패 원인(헤더)을 추출하여 모니터링/알림 연동 가능
 *   - 수동 재처리가 필요한 경우 DLT 메시지를 원본 토픽으로 재발행
 */
@Component
class SettlementDltConsumer(
    private val objectMapper: ObjectMapper,
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

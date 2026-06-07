package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.core.enums.NotificationType
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.notificationworker.service.NotificationService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 2단계(해결): DLT 컨슈머 — 재시도 3회 소진 후 DLT 로 이동된 메시지 처리
 *
 * DLT 이동 조건:
 *   - 1초 간격 3회 재시도를 모두 소진한 메시지 (KafkaConsumerConfig 참고)
 *   - 비즈니스 예외/외부 서비스 장애 등 모든 RuntimeException 대상
 *   - DataIntegrityViolationException(중복 소비) 은 DLT 이동 대상이 아님
 *     → NotificationConsumer 에서 consumer 레벨 catch 로 DLT 미발동 처리
 *
 * 처리 방침:
 *   - notification 테이블에 FAILED 상태로 저장 → 수동 재처리/모니터링 이력 보존
 *   - JSON 파싱 실패 시 paymentId 특정 불가 → DB 저장 없이 로그만 남김
 *   - DLT 메시지 중복 수신 시(unique constraint 위반) idempotency skip
 */
@Component
class DltNotificationConsumer(
    private val notificationService: NotificationService,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["payment.approved.DLT"], groupId = "notification-dlt-group")
    fun onPaymentApprovedDlt(record: ConsumerRecord<String, String>) {
        val failureReason = extractFailureReason(record)
        val event = parseEvent(record) ?: run {
            // JSON 파싱 실패 시 paymentId 특정 불가 → DB 저장 없이 로그만 남김
            log.error("[dlt] 이벤트 파싱 실패 — 수동 처리 필요 value={}", record.value())
            return
        }

        log.warn("[dlt] payment.approved.DLT 수신 paymentId={} reason={}", event.paymentId, failureReason)

        try {
            notificationService.saveFailedNotification(event.paymentId, NotificationType.PAYMENT_COMPLETED, failureReason)
        } catch (e: DataIntegrityViolationException) {
            // 이미 SENT 또는 FAILED 레코드 존재 (unique constraint) → idempotency skip
            log.warn("[dlt] 이미 처리된 알림 skip paymentId={} type=PAYMENT_COMPLETED", event.paymentId)
        }
    }

    @KafkaListener(topics = ["payment.canceled.DLT"], groupId = "notification-dlt-group")
    fun onPaymentCanceledDlt(record: ConsumerRecord<String, String>) {
        val failureReason = extractFailureReason(record)
        val event = parseEvent(record) ?: run {
            log.error("[dlt] 이벤트 파싱 실패 — 수동 처리 필요 value={}", record.value())
            return
        }

        log.warn("[dlt] payment.canceled.DLT 수신 paymentId={} reason={}", event.paymentId, failureReason)

        try {
            notificationService.saveFailedNotification(event.paymentId, NotificationType.PAYMENT_CANCELED, failureReason)
        } catch (e: DataIntegrityViolationException) {
            // 이미 SENT 또는 FAILED 레코드 존재 (unique constraint) → idempotency skip
            log.warn("[dlt] 이미 처리된 알림 skip paymentId={} type=PAYMENT_CANCELED", event.paymentId)
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

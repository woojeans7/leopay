package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.NotificationType
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.storage.entity.NotificationEntity
import com.leopay.storage.repository.NotificationRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

/**
 * A-4 2단계(해결): DLT 컨슈머 — 재시도 3회 소진 후 DLT 로 이동된 메시지 처리
 *
 * DLT 이동 조건:
 *   - 1초 간격 3회 재시도를 모두 소진한 메시지 (KafkaConsumerConfig 참고)
 *   - 비즈니스 예외/외부 서비스 장애 등 모든 RuntimeException 대상
 *
 * 처리 방침:
 *   - notification 테이블에 FAILED 상태로 저장 → 수동 재처리/모니터링 이력 보존
 *   - DLT 이동 시점의 실패 원인(헤더)을 failureReason 으로 기록
 *
 * B-3 멱등성: DLT 메시지도 중복 수신 가능 (at-least-once) — FAILED 저장은 알림 미발송 이력이므로
 *   중복 저장이 발생해도 운영 영향 없음 (수동 재처리 시 중복 체크)
 */
@Component
class DltNotificationConsumer(
    private val notificationRepository: NotificationRepository,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["payment.approved.DLT"], groupId = "notification-dlt-group")
    @Transactional
    fun onPaymentApprovedDlt(record: ConsumerRecord<String, String>) {
        val failureReason = extractFailureReason(record)
        val event = parseEvent(record)

        log.warn(
            "[dlt] payment.approved.DLT 수신 paymentId={} reason={}",
            event?.paymentId, failureReason
        )

        saveFailedNotification(
            paymentId = event?.paymentId ?: -1L,
            type = NotificationType.PAYMENT_COMPLETED,
            failureReason = failureReason,
        )
    }

    @KafkaListener(topics = ["payment.canceled.DLT"], groupId = "notification-dlt-group")
    @Transactional
    fun onPaymentCanceledDlt(record: ConsumerRecord<String, String>) {
        val failureReason = extractFailureReason(record)
        val event = parseEvent(record)

        log.warn(
            "[dlt] payment.canceled.DLT 수신 paymentId={} reason={}",
            event?.paymentId, failureReason
        )

        saveFailedNotification(
            paymentId = event?.paymentId ?: -1L,
            type = NotificationType.PAYMENT_CANCELED,
            failureReason = failureReason,
        )
    }

    private fun parseEvent(record: ConsumerRecord<String, String>): PaymentEvent? {
        return runCatching {
            objectMapper.readValue(record.value(), PaymentEvent::class.java)
        }.getOrElse { ex ->
            log.error("[dlt] 메시지 파싱 실패 value={} error={}", record.value(), ex.message)
            null
        }
    }

    /** DLT 헤더에서 Spring Kafka 가 자동으로 첨부하는 원인 메시지를 추출한다. */
    private fun extractFailureReason(record: ConsumerRecord<String, String>): String {
        val header = record.headers().lastHeader("kafka_dlt-exception-message")
        return header?.value()?.let { String(it) } ?: "unknown"
    }

    private fun saveFailedNotification(
        paymentId: Long,
        type: NotificationType,
        failureReason: String,
    ) {
        notificationRepository.save(
            NotificationEntity(
                paymentId = paymentId,
                type = type,
                status = NotificationStatus.FAILED,
                failureReason = failureReason.take(500),
            )
        )
        log.info("[dlt] FAILED 알림 이력 저장 paymentId={} type={}", paymentId, type)
    }
}

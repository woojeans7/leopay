package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.notificationworker.service.NotificationService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 1단계(문제 재현): 에러 핸들러/DLT 설정 없음
 *
 * 처리 중 예외 발생 시 Spring Kafka 기본 동작:
 *   DefaultErrorHandler → 최대 9회 재시도 후 offset commit → 메시지 영구 유실
 *   DLT 없음 → 실패한 메시지 추적/재처리 불가 → 알림 미발송
 *
 * 2단계에서 DefaultErrorHandler(3회) + DeadLetterPublishingRecoverer 추가로 해결
 */
@Component
class NotificationConsumer(
    private val notificationService: NotificationService,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["payment.approved"])
    fun onPaymentApproved(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)
        log.info("[notification] payment.approved paymentId={}", event.paymentId)
        notificationService.sendApprovedNotification(event.paymentId)
    }

    @KafkaListener(topics = ["payment.canceled"])
    fun onPaymentCanceled(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)
        log.info("[notification] payment.canceled paymentId={}", event.paymentId)
        notificationService.sendCanceledNotification(event.paymentId)
    }
}

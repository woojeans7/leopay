package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.notificationworker.service.NotificationService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * A-4 2단계(해결 완료): KafkaConsumerConfig 에 DefaultErrorHandler + DLT 적용
 *
 * 1단계 문제:
 *   에러 핸들러/DLT 설정 없음 → 기본 9회 재시도 후 offset commit → 메시지 영구 유실
 *   DLT 없음 → 실패한 메시지 추적/재처리 불가 → 알림 미발송
 *
 * 2단계 해결:
 *   - KafkaConsumerConfig: DefaultErrorHandler(FixedBackOff 1초 × 3회) + DeadLetterPublishingRecoverer
 *   - 재시도 3회 소진 시 <토픽>.DLT 로 이동 → DltNotificationConsumer 가 FAILED 이력 저장
 *   - 메시지 유실 0건, 수동 재처리 이력 보존
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

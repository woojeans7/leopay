package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.notificationworker.service.NotificationService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
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
 *
 * B-3 시나리오(예방 설계): Kafka at-least-once 중복 소비 방지
 *
 *   문제: 컨슈머 재시작, 리밸런싱, 네트워크 이슈 등으로 같은 메시지가 2회 이상 수신될 수 있음
 *        → 알림 중복 발송, notification 레코드 중복 생성
 *
 *   해결: check+save 를 동일 트랜잭션(service) 에서 처리하여 TOCTOU 제거
 *        DB unique constraint 위반(DataIntegrityViolationException) 은 consumer 레벨에서 catch
 *        → 정상 idempotency skip 으로 처리, DLT 미발동
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
        try {
            notificationService.sendApprovedNotification(event.paymentId)
        } catch (e: DataIntegrityViolationException) {
            // 동시 중복 소비 — unique constraint 위반은 정상 idempotency skip
            // 트랜잭션 경계 바깥에서 catch → DLT 미발동
            log.warn("[idempotency] 중복 소비 skip paymentId={}", event.paymentId)
        }
    }

    @KafkaListener(topics = ["payment.canceled"])
    fun onPaymentCanceled(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)
        log.info("[notification] payment.canceled paymentId={}", event.paymentId)
        try {
            notificationService.sendCanceledNotification(event.paymentId)
        } catch (e: DataIntegrityViolationException) {
            // 동시 중복 소비 — unique constraint 위반은 정상 idempotency skip
            // 트랜잭션 경계 바깥에서 catch → DLT 미발동
            log.warn("[idempotency] 중복 소비 skip paymentId={}", event.paymentId)
        }
    }
}

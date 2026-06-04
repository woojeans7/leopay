package com.leopay.notificationworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.NotificationType
import com.leopay.notificationworker.dto.PaymentEvent
import com.leopay.notificationworker.service.NotificationService
import com.leopay.storage.repository.NotificationRepository
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
 *
 * B-3 시나리오(예방 설계): Kafka at-least-once 중복 소비 방지
 *
 *   문제: 컨슈머 재시작, 리밸런싱, 네트워크 이슈 등으로 같은 메시지가 2회 이상 수신될 수 있음
 *        → 알림 중복 발송, notification 레코드 중복 생성
 *
 *   해결: paymentId + NotificationType 조합을 멱등성 키로 사용
 *        처리 전 DB 에서 SENT 레코드 존재 여부 확인 → 존재하면 skip
 *        DB unique constraint (payment_id, type) 가 최종 방어선 역할
 */
@Component
class NotificationConsumer(
    private val notificationService: NotificationService,
    private val notificationRepository: NotificationRepository,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["payment.approved"])
    fun onPaymentApproved(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)

        // B-3: SENT 레코드가 이미 있으면 동일 이벤트를 정상 처리한 것이므로 skip
        if (notificationRepository.existsByPaymentIdAndTypeAndStatus(
                event.paymentId, NotificationType.PAYMENT_COMPLETED, NotificationStatus.SENT
            )
        ) {
            log.warn("[idempotency] 중복 메시지 skip paymentId={} type=PAYMENT_COMPLETED", event.paymentId)
            return
        }

        log.info("[notification] payment.approved paymentId={}", event.paymentId)
        notificationService.sendApprovedNotification(event.paymentId)
    }

    @KafkaListener(topics = ["payment.canceled"])
    fun onPaymentCanceled(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)

        // B-3: SENT 레코드가 이미 있으면 동일 이벤트를 정상 처리한 것이므로 skip
        if (notificationRepository.existsByPaymentIdAndTypeAndStatus(
                event.paymentId, NotificationType.PAYMENT_CANCELED, NotificationStatus.SENT
            )
        ) {
            log.warn("[idempotency] 중복 메시지 skip paymentId={} type=PAYMENT_CANCELED", event.paymentId)
            return
        }

        log.info("[notification] payment.canceled paymentId={}", event.paymentId)
        notificationService.sendCanceledNotification(event.paymentId)
    }
}

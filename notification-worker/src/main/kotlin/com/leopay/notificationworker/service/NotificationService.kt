package com.leopay.notificationworker.service

import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.NotificationType
import com.leopay.storage.entity.NotificationEntity
import com.leopay.storage.repository.NotificationRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class NotificationService(
    private val notificationRepository: NotificationRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun sendApprovedNotification(paymentId: Long) {
        // B-3: check+save 동일 트랜잭션 안에서 처리 (TOCTOU 방지)
        if (notificationRepository.existsByPaymentIdAndTypeAndStatus(
                paymentId, NotificationType.PAYMENT_COMPLETED, NotificationStatus.SENT
            )
        ) {
            log.warn("[idempotency] 중복 skip paymentId={} type=PAYMENT_COMPLETED", paymentId)
            return
        }
        val notification = notificationRepository.save(
            NotificationEntity(
                paymentId = paymentId,
                type = NotificationType.PAYMENT_COMPLETED,
                status = NotificationStatus.PENDING,
            )
        )
        log.info("[notification] 결제 완료 알림 발송 paymentId={}", paymentId)
        notification.status = NotificationStatus.SENT
        notification.sentAt = LocalDateTime.now()
    }

    @Transactional
    fun sendCanceledNotification(paymentId: Long) {
        // B-3: check+save 동일 트랜잭션 안에서 처리 (TOCTOU 방지)
        if (notificationRepository.existsByPaymentIdAndTypeAndStatus(
                paymentId, NotificationType.PAYMENT_CANCELED, NotificationStatus.SENT
            )
        ) {
            log.warn("[idempotency] 중복 skip paymentId={} type=PAYMENT_CANCELED", paymentId)
            return
        }
        val notification = notificationRepository.save(
            NotificationEntity(
                paymentId = paymentId,
                type = NotificationType.PAYMENT_CANCELED,
                status = NotificationStatus.PENDING,
            )
        )
        log.info("[notification] 결제 취소 알림 발송 paymentId={}", paymentId)
        notification.status = NotificationStatus.SENT
        notification.sentAt = LocalDateTime.now()
    }

    // A-4: DLT consumer용 — FAILED 이력 저장
    @Transactional
    fun saveFailedNotification(paymentId: Long, type: NotificationType, failureReason: String) {
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

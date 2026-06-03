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
}

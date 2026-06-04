package com.leopay.storage.repository

import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.NotificationType
import com.leopay.storage.entity.NotificationEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository

interface NotificationRepository : JpaRepository<NotificationEntity, Long> {

    fun findByStatusOrderByCreatedAtAsc(status: NotificationStatus, pageable: Pageable): List<NotificationEntity>

    fun findByPaymentId(paymentId: Long): List<NotificationEntity>

    fun countByStatus(status: NotificationStatus): Long

    /**
     * B-3 멱등성 체크: paymentId + type 조합으로 이미 처리된 이벤트인지 확인한다.
     * SENT 상태인 레코드가 존재하면 같은 이벤트가 이미 정상 처리된 것으로 판단한다.
     */
    fun existsByPaymentIdAndTypeAndStatus(
        paymentId: Long,
        type: NotificationType,
        status: NotificationStatus,
    ): Boolean
}

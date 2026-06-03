package com.leopay.storage.repository

import com.leopay.core.enums.NotificationStatus
import com.leopay.storage.entity.NotificationEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository

interface NotificationRepository : JpaRepository<NotificationEntity, Long> {

    fun findByStatusOrderByCreatedAtAsc(status: NotificationStatus, pageable: Pageable): List<NotificationEntity>

    fun findByPaymentId(paymentId: Long): List<NotificationEntity>

    fun countByStatus(status: NotificationStatus): Long
}

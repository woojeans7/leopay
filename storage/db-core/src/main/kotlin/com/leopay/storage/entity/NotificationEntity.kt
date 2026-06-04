package com.leopay.storage.entity

import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.NotificationType
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.time.LocalDateTime

@Entity
@Table(
    name = "notification",
    uniqueConstraints = [
        // B-3: paymentId + type 조합 유일성 → DB 레벨 중복 저장 최종 방어선
        UniqueConstraint(name = "uk_notification_payment_type", columnNames = ["payment_id", "type"]),
    ]
)
class NotificationEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "payment_id", nullable = false)
    val paymentId: Long,

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 20)
    val type: NotificationType,

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    var status: NotificationStatus,

    @Column(name = "retry_count", nullable = false)
    var retryCount: Int = 0,

    @Column(name = "failure_reason", length = 500)
    var failureReason: String? = null,

    @Column(name = "sent_at")
    var sentAt: LocalDateTime? = null,

) : BaseEntity()

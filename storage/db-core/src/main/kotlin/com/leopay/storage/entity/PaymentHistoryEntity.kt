package com.leopay.storage.entity

import com.leopay.core.enums.PaymentStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityListeners
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.annotation.CreatedDate
import org.springframework.data.jpa.domain.support.AuditingEntityListener
import java.time.LocalDateTime

@Entity
@Table(name = "payment_history")
@EntityListeners(AuditingEntityListener::class)
class PaymentHistoryEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "payment_id", nullable = false)
    val paymentId: Long,

    @Enumerated(EnumType.STRING)
    @Column(name = "previous_status", length = 20)
    val previousStatus: PaymentStatus? = null,

    @Enumerated(EnumType.STRING)
    @Column(name = "new_status", nullable = false, length = 20)
    val newStatus: PaymentStatus,

    @Column(name = "reason", length = 500)
    val reason: String? = null,

) {
    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    lateinit var createdAt: LocalDateTime
}

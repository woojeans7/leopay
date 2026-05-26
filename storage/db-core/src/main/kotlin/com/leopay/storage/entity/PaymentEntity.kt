package com.leopay.storage.entity

import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "payment")
class PaymentEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "payment_key", nullable = false, unique = true, length = 64)
    val paymentKey: String,

    @Column(name = "merchant_id", nullable = false)
    val merchantId: Long,

    @Column(name = "user_id", nullable = false, length = 64)
    val userId: String,

    @Column(name = "amount", nullable = false, precision = 12, scale = 0)
    val amount: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(name = "method", nullable = false, length = 20)
    val method: PaymentMethod,

    status: PaymentStatus,

    @Column(name = "pg_transaction_id", length = 64)
    var pgTransactionId: String? = null,

    @Column(name = "cancel_reason", length = 200)
    var cancelReason: String? = null,

    @Column(name = "approved_at")
    var approvedAt: LocalDateTime? = null,

    @Column(name = "canceled_at")
    var canceledAt: LocalDateTime? = null,

) : BaseEntity() {

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    var status: PaymentStatus = status
        private set

    fun transitionTo(next: PaymentStatus) {
        require(status.canTransitionTo(next)) { "Invalid transition: $status -> $next" }
        status = next
    }
}

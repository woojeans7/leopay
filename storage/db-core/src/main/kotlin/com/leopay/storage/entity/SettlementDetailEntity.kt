package com.leopay.storage.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityListeners
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.annotation.CreatedDate
import org.springframework.data.jpa.domain.support.AuditingEntityListener
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "settlement_detail")
@EntityListeners(AuditingEntityListener::class)
class SettlementDetailEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "settlement_id", nullable = false)
    val settlementId: Long,

    @Column(name = "payment_id", nullable = false)
    val paymentId: Long,

    @Column(name = "amount", nullable = false, precision = 12, scale = 0)
    val amount: BigDecimal,

    @Column(name = "fee_amount", nullable = false, precision = 15, scale = 4)
    val feeAmount: BigDecimal,

) {
    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    lateinit var createdAt: LocalDateTime
}

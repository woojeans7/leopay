package com.leopay.storage.entity

import com.leopay.core.enums.SettlementStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

@Entity
@Table(
    name = "settlement",
    uniqueConstraints = [UniqueConstraint(name = "uk_settlement_merchant_date", columnNames = ["merchant_id", "settlement_date"])]
)
class SettlementEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "merchant_id", nullable = false)
    val merchantId: Long,

    @Column(name = "settlement_date", nullable = false)
    val settlementDate: LocalDate,

    @Column(name = "total_count", nullable = false)
    var totalCount: Int = 0,

    @Column(name = "total_amount", nullable = false, precision = 15, scale = 0)
    var totalAmount: BigDecimal = BigDecimal.ZERO,

    @Column(name = "fee_amount", nullable = false, precision = 15, scale = 4)
    var feeAmount: BigDecimal = BigDecimal.ZERO,

    @Column(name = "settlement_amount", nullable = false, precision = 15, scale = 0)
    var settlementAmount: BigDecimal = BigDecimal.ZERO,

    @Column(name = "applied_fee_rate", nullable = false, precision = 5, scale = 4)
    val appliedFeeRate: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    var status: SettlementStatus,

    @Column(name = "completed_at")
    var completedAt: LocalDateTime? = null,

    @Column(name = "transferred_at")
    var transferredAt: LocalDateTime? = null,

) : BaseEntity()

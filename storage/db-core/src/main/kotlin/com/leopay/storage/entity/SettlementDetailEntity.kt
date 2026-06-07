package com.leopay.storage.entity

import com.leopay.core.enums.SettlementStatus
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
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

@Entity
@Table(name = "settlement_detail")
@EntityListeners(AuditingEntityListener::class)
class SettlementDetailEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    // 배치 완료 후 연결 — Consumer 선적재 시점에는 null
    @Column(name = "settlement_id", nullable = true)
    var settlementId: Long? = null,

    @Column(name = "payment_id", nullable = false)
    val paymentId: Long,

    @Column(name = "merchant_id", nullable = false)
    val merchantId: Long,

    // 결제 승인일 기준 정산 기준일
    @Column(name = "settlement_date", nullable = false)
    val settlementDate: LocalDate,

    @Column(name = "amount", nullable = false, precision = 12, scale = 0)
    val amount: BigDecimal,

    @Column(name = "fee_amount", nullable = false, precision = 15, scale = 4)
    val feeAmount: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    var status: SettlementStatus = SettlementStatus.PENDING,

) {
    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    lateinit var createdAt: LocalDateTime
}

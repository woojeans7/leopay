package com.leopay.storage.entity

import com.leopay.core.enums.MerchantStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.math.BigDecimal

@Entity
@Table(name = "merchant")
class MerchantEntity(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "business_number", nullable = false, unique = true, length = 20)
    val businessNumber: String,

    @Column(name = "name", nullable = false, length = 100)
    var name: String,

    @Column(name = "bank_code", nullable = false, length = 50)
    var bankCode: String,

    @Column(name = "account_number", nullable = false, length = 30)
    var accountNumber: String,

    @Column(name = "account_holder", nullable = false, length = 100)
    var accountHolder: String,

    @Column(name = "fee_rate", nullable = false, precision = 5, scale = 4)
    var feeRate: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    var status: MerchantStatus,

) : BaseEntity()

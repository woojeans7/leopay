package com.leopay.storage.repository

import com.leopay.core.enums.SettlementStatus
import com.leopay.storage.entity.SettlementEntity
import org.springframework.data.jpa.repository.JpaRepository
import java.time.LocalDate
import java.util.Optional

interface SettlementRepository : JpaRepository<SettlementEntity, Long> {

    fun findByMerchantIdAndSettlementDate(merchantId: Long, settlementDate: LocalDate): Optional<SettlementEntity>

    fun findByStatus(status: SettlementStatus): List<SettlementEntity>
}

package com.leopay.storage.repository

import com.leopay.storage.entity.SettlementDetailEntity
import org.springframework.data.jpa.repository.JpaRepository

interface SettlementDetailRepository : JpaRepository<SettlementDetailEntity, Long> {

    fun findBySettlementId(settlementId: Long): List<SettlementDetailEntity>
}

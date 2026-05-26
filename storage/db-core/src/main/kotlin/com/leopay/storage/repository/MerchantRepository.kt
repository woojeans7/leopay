package com.leopay.storage.repository

import com.leopay.storage.entity.MerchantEntity
import org.springframework.data.jpa.repository.JpaRepository
import java.util.Optional

interface MerchantRepository : JpaRepository<MerchantEntity, Long> {

    fun findByBusinessNumber(businessNumber: String): Optional<MerchantEntity>
}

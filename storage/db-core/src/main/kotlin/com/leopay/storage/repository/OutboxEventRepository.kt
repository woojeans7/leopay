package com.leopay.storage.repository

import com.leopay.storage.entity.OutboxEventEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository

interface OutboxEventRepository : JpaRepository<OutboxEventEntity, Long> {

    fun findByPublishedFalseOrderByCreatedAtAsc(pageable: Pageable): List<OutboxEventEntity>
}

package com.leopay.bookingapi.outbox

import com.leopay.storage.repository.OutboxEventRepository
import org.springframework.data.domain.PageRequest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Component
class OutboxPublisher(
    private val outboxEventRepository: OutboxEventRepository,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    companion object {
        private fun topicName(eventType: String): String =
            eventType.lowercase().replace('_', '.')
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    fun publish() {
        val events = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc(
            PageRequest.of(0, 100)
        )

        for (event in events) {
            kafkaTemplate.send(topicName(event.eventType), event.payload)
            event.published = true
            event.publishedAt = LocalDateTime.now()
        }
    }
}

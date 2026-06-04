package com.leopay.notificationworker.config

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

/**
 * A-4 2단계(해결): Kafka 컨슈머 에러 핸들러 + DLT 설정
 *
 * 문제(1단계): 에러 핸들러 미설정 → 기본 9회 재시도 후 offset commit → 메시지 영구 유실
 *
 * 해결:
 *   - FixedBackOff(1000L, 3L): 1초 간격 최대 3회 재시도 (총 4회 처리 시도)
 *   - DeadLetterPublishingRecoverer: 3회 재시도 소진 시 <토픽명>.DLT 로 이동
 *     → DltNotificationConsumer 가 FAILED 이력 저장, 수동 재처리 가능
 *   - 실패 메시지가 DLT 에 보존되므로 알림 유실 0건 달성
 */
@Configuration
class KafkaConsumerConfig {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * DeadLetterPublishingRecoverer: 기본 동작으로 <원본토픽>.DLT 에 발행
     *   - payment.approved  → payment.approved.DLT
     *   - payment.canceled  → payment.canceled.DLT
     *
     * Spring Boot 자동 구성 KafkaTemplate<String, String> 을 unchecked cast 로 수용한다.
     * DeadLetterPublishingRecoverer 는 내부적으로 KafkaOperations<Any, Any> 를 사용하지만
     * String 페이로드를 그대로 전달하므로 런타임 오류 없음.
     */
    @Bean
    @Suppress("UNCHECKED_CAST")
    fun deadLetterPublishingRecoverer(
        kafkaTemplate: KafkaTemplate<String, String>,
    ): DeadLetterPublishingRecoverer =
        DeadLetterPublishingRecoverer(kafkaTemplate as KafkaTemplate<Any, Any>) { cr, _ ->
            TopicPartition("${cr.topic()}.DLT", cr.partition())
        }

    /**
     * DefaultErrorHandler:
     *   - FixedBackOff(1000L, 3L) — 1초 간격, 최대 3회 재시도
     *   - 재시도 소진 시 DeadLetterPublishingRecoverer 로 DLT 이동
     */
    @Bean
    fun defaultErrorHandler(
        recoverer: DeadLetterPublishingRecoverer,
    ): DefaultErrorHandler {
        val backOff = FixedBackOff(1_000L, 3L)
        val errorHandler = DefaultErrorHandler(recoverer, backOff)
        errorHandler.setRetryListeners(
            object : org.springframework.kafka.listener.RetryListener {
                override fun failedDelivery(
                    record: ConsumerRecord<*, *>,
                    ex: Exception,
                    deliveryAttempt: Int,
                ) {
                    log.warn(
                        "[dlt] 재시도 중 paymentId={} attempt={} error={}",
                        record.key(), deliveryAttempt, ex.message,
                    )
                }
            }
        )
        return errorHandler
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        errorHandler: DefaultErrorHandler,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }
}

package com.leopay.settlementworker.config

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
 *             PaymentSettlementConsumer 는 에러 핸들러 없이 동작하여 실패 시 정산 누락 재현용
 *
 * 해결:
 *   - FixedBackOff(1000L, 3L): 1초 간격 최대 3회 재시도 (총 4회 처리 시도)
 *   - DeadLetterPublishingRecoverer: 3회 재시도 소진 시 payment.approved.DLT 로 이동
 *     → SettlementDltConsumer 가 WARN 로그 기록, 수동 재처리 가능
 *   - 실패 메시지가 DLT 에 보존되므로 정산 유실 0건 달성
 *
 * 주의: PaymentSettlementConsumer 는 이 ErrorHandler 를 사용하지 않는다.
 *       kafkaListenerContainerFactory 를 명시하지 않은 리스너는 Spring Boot 자동 구성
 *       기본 컨테이너 팩토리를 사용한다 — A-4 1단계(문제 재현) 목적.
 */
@Configuration
class KafkaConsumerConfig {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * DeadLetterPublishingRecoverer: payment.approved → payment.approved.DLT
     *
     * Spring Boot 자동 구성 KafkaTemplate<String, String> 을 unchecked cast 로 수용한다.
     */
    @Bean
    @Suppress("UNCHECKED_CAST")
    fun settlementDeadLetterPublishingRecoverer(
        kafkaTemplate: KafkaTemplate<String, String>,
    ): DeadLetterPublishingRecoverer =
        DeadLetterPublishingRecoverer(kafkaTemplate as KafkaTemplate<Any, Any>) { cr, _ ->
            TopicPartition("${cr.topic()}.DLT", cr.partition())
        }

    /**
     * DefaultErrorHandler:
     *   - FixedBackOff(1000L, 3L) — 1초 간격, 최대 3회 재시도
     *   - 재시도 소진 시 DeadLetterPublishingRecoverer 로 payment.approved.DLT 이동
     */
    @Bean
    fun settlementErrorHandler(
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
                        "[dlt] 정산 재시도 중 paymentId={} attempt={} error={}",
                        record.key(), deliveryAttempt, ex.message,
                    )
                }
            }
        )
        return errorHandler
    }

    /**
     * DLT 전용 컨테이너 팩토리 — SettlementDltConsumer 에서 containerFactory 로 명시
     */
    @Bean
    fun settlementDltListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        errorHandler: DefaultErrorHandler,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }
}

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
 *             테스트에서 @TestConfiguration 으로 에러 핸들러 없는 팩토리를 override 하여 재현
 *             (SettlementMessageLossTest.FastErrorHandlerConfig 패턴)
 *
 * 해결:
 *   - FixedBackOff(1000L, 3L): 1초 간격 최대 3회 재시도 (총 4회 처리 시도)
 *   - DeadLetterPublishingRecoverer: 3회 재시도 소진 시 payment.approved.DLT 로 이동
 *     → SettlementDltConsumer 가 settlement_detail 재적재 후 배치 집계 가능
 *   - 실패 메시지가 DLT 에 보존되므로 정산 유실 0건 달성
 *
 * 팩토리 역할 분리:
 *   - settlementListenerContainerFactory: 에러 핸들러 포함 → PaymentSettlementConsumer 전용
 *   - settlementDltListenerContainerFactory: 단순 팩토리 (에러 핸들러 없음) → SettlementDltConsumer 전용
 *     DLT Consumer 자체에 에러 핸들러를 달면 실패 시 또 DLT 로 보내는 순환 구조가 되므로 제거
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
     * 원본 컨슈머 전용 컨테이너 팩토리 — PaymentSettlementConsumer 에서 containerFactory 로 명시
     *
     * 에러 핸들러 포함: 3회 재시도 후 payment.approved.DLT 로 이동
     */
    @Bean
    fun settlementListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        errorHandler: DefaultErrorHandler,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }

    /**
     * DLT 전용 컨테이너 팩토리 — SettlementDltConsumer 에서 containerFactory 로 명시
     *
     * 에러 핸들러 없음: DLT Consumer 실패 시 재처리 로직은 수동 운영 대응
     * (DLT Consumer 에 에러 핸들러를 달면 실패 시 또 DLT 로 이동하는 순환 구조가 됨)
     */
    @Bean
    fun settlementDltListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        return factory
    }
}

package com.leopay.settlementworker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.settlementworker.dto.PaymentEvent
import com.leopay.storage.repository.PaymentRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.redisson.api.RedissonClient
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.util.UUID

/**
 * A-4 1단계(문제 재현): 에러 핸들러 없는 기본 컨슈머
 *
 * 이 컨슈머는 의도적으로 DLT 에러 핸들러를 달지 않는다.
 * 처리 실패 시 기본 동작(9회 재시도 후 offset commit)으로 메시지가 영구 유실된다.
 * → 정산 누락이 발생하는 문제 상황을 재현하기 위한 용도.
 *
 * B-3 시나리오(예방 설계): Kafka at-least-once 중복 소비 방지
 *   - 키: settlement:processed:{paymentId}
 *   - Redis Set SISMEMBER 확인 → 이미 처리됐으면 skip
 *   - 처리 완료 후 SADD
 */
@Component
class PaymentSettlementConsumer(
    private val objectMapper: ObjectMapper,
    private val paymentRepository: PaymentRepository,
    private val redissonClient: RedissonClient,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // batch-implementer 가 구현할 settlementJob 을 선택적으로 주입
    // Job 이 아직 없어도 컨슈머가 기동되도록 required = false
    @Autowired(required = false)
    private var jobLauncher: JobLauncher? = null

    @Autowired(required = false)
    private var settlementJob: Job? = null

    companion object {
        private const val IDEMPOTENCY_KEY_PREFIX = "settlement:processed:"
    }

    /**
     * payment.approved 토픽 컨슈머
     *
     * NOTE: containerFactory 미지정 → Spring Boot 기본 컨테이너 팩토리 사용
     *       (에러 핸들러 없음 — A-4 메시지 유실 재현 목적)
     */
    @KafkaListener(
        topics = ["payment.approved"],
        groupId = "settlement-consumer-group",
    )
    fun onPaymentApproved(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), PaymentEvent::class.java)
        val paymentId = event.paymentId

        // B-3: Redis Set 으로 중복 소비 방지
        val idempotencyKey = "$IDEMPOTENCY_KEY_PREFIX$paymentId"
        val processedSet = redissonClient.getSet<String>("settlement:processed")
        if (processedSet.contains(idempotencyKey)) {
            log.warn("[idempotency] 중복 메시지 skip paymentId={}", paymentId)
            return
        }

        log.info("[settlement] payment.approved 수신 paymentId={}", paymentId)

        val payment = paymentRepository.findById(paymentId).orElseThrow {
            IllegalArgumentException("Payment not found: paymentId=$paymentId")
        }

        val merchantId = payment.merchantId
        val settlementDate = LocalDate.now()

        triggerSettlementJob(merchantId, settlementDate, paymentId)

        // B-3: 처리 완료 후 Redis 에 기록
        processedSet.add(idempotencyKey)
        log.info("[settlement] 정산 트리거 완료 paymentId={} merchantId={}", paymentId, merchantId)
    }

    private fun triggerSettlementJob(merchantId: Long, settlementDate: LocalDate, paymentId: Long) {
        val launcher = jobLauncher
        val job = settlementJob

        if (launcher == null || job == null) {
            // batch-implementer 구현 전 단계: 잡이 없으면 경고 로그만 남김
            log.warn(
                "[settlement] settlementJob 미구현 — 정산 트리거 skip paymentId={} merchantId={}",
                paymentId, merchantId
            )
            return
        }

        val jobParameters = JobParametersBuilder()
            .addLong("merchantId", merchantId)
            .addString("settlementDate", settlementDate.toString())
            .addString("runId", UUID.randomUUID().toString()) // 재실행 허용용 uuid
            .toJobParameters()

        launcher.run(job, jobParameters)
        log.info(
            "[settlement] settlementJob 실행 merchantId={} settlementDate={} paymentId={}",
            merchantId, settlementDate, paymentId
        )
    }
}

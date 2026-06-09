package com.leopay.bookingapi.idempotency

import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.outbox.OutboxPublisher
import com.leopay.bookingapi.service.PaymentService
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.NotificationRepository
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import com.leopay.bookingapi.exception.PaymentException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.redisson.api.RedissonClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.math.BigDecimal
import java.time.LocalDateTime

/**
 * A-2 시나리오: 멱등성(Idempotency-Key) 적용 후 클라이언트 재시도 검증
 *
 * 문제(수정 전): 네트워크 유실 후 재시도 시 DuplicatePayment(409) / InvalidStatus(422) 반환
 *              → 클라이언트가 실패로 오판 → 재결제 → 이중과금 위험
 * 해결(수정 후):
 *   - createPayment: paymentKey를 멱등성 키로 Redis 캐싱 → 재시도 시 첫 응답 그대로 반환
 *   - approvePayment: Idempotency-Key 헤더 기반 캐싱 제거, 상태 머신이 중복 승인을 차단
 *                    (APPROVED 상태에서 재시도 시 InvalidStatus 예외 발생)
 *
 * develop test: 로컬 MySQL(localhost:3306) + Redis(localhost:6379) 실제 연결 필요
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = [KafkaAutoConfiguration::class])
@ActiveProfiles("test")
class IdempotencyRetryTest {

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentRepository: PaymentRepository

    @Autowired
    private lateinit var paymentHistoryRepository: PaymentHistoryRepository

    @Autowired
    private lateinit var outboxEventRepository: OutboxEventRepository

    @Autowired
    private lateinit var settlementDetailRepository: SettlementDetailRepository

    @Autowired
    private lateinit var notificationRepository: NotificationRepository

    @Autowired
    private lateinit var txManager: PlatformTransactionManager

    @Autowired
    private lateinit var redissonClient: RedissonClient

    @MockkBean
    private lateinit var outboxPublisher: OutboxPublisher

    @MockkBean
    private lateinit var gateway: PaymentGateway

    @BeforeEach
    fun setUp() {
        redissonClient.keys.deleteByPattern("idempotency:*")
        settlementDetailRepository.deleteAll()
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()

        coEvery { gateway.approve(any()) } returns PgApproveResponse(
            pgTransactionId = "PG-test-${System.nanoTime()}",
            approvedAt = LocalDateTime.now(),
        )
    }

    /**
     * createPayment 재시도 — 멱등성 적용 후 검증:
     * 동일 paymentKey로 재시도하면 1차 요청과 동일한 응답을 반환하고 결제 레코드는 1건만 존재한다.
     */
    @Test
    fun `createPayment 재시도 - 멱등성 적용 후 동일 응답 반환`() {
        val request = PaymentCreateRequest(
            paymentKey = "idempotency-test-create",
            merchantId = 1L,
            amount = BigDecimal("10000"),
            method = PaymentMethod.CARD,
        )

        val firstResponse = paymentService.createPayment("user-1", request)
        val retryResponse = paymentService.createPayment("user-1", request)

        assertEquals(firstResponse.paymentId, retryResponse.paymentId, "재시도 응답의 paymentId는 1차 응답과 동일해야 한다")
        assertEquals(firstResponse.status, retryResponse.status)
        assertEquals(1L, paymentRepository.count(), "결제 레코드는 1건만 존재해야 한다")
    }

    /**
     * approvePayment 재시도 — 상태 머신이 중복 승인을 차단하는 검증:
     * 이미 APPROVED 상태인 결제에 다시 approvePayment를 호출하면 InvalidStatus 예외가 발생한다.
     * Idempotency-Key 캐싱 대신 상태 머신이 중복 승인 방어선 역할을 담당한다.
     */
    @Test
    fun `approvePayment 재시도 - 상태 머신이 중복 승인을 차단한다`() {
        // APPROVED 상태 결제 직접 세팅
        val txTemplate = TransactionTemplate(txManager)
        val paymentId = txTemplate.execute {
            paymentRepository.save(
                PaymentEntity(
                    paymentKey = "idempotency-test-approve",
                    merchantId = 1L,
                    userId = "user-1",
                    amount = BigDecimal("50000"),
                    method = PaymentMethod.CARD,
                    status = PaymentStatus.APPROVED,  // 이미 APPROVED
                )
            ).id!!
        }!!

        assertThrows<PaymentException.InvalidStatus> {
            paymentService.approvePayment(paymentId)
        }
    }
}

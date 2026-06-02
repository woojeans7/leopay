package com.leopay.bookingapi.idempotency

import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.exception.PaymentException
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.outbox.OutboxPublisher
import com.leopay.bookingapi.service.PaymentService
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
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
 * A-2 시나리오: 클라이언트 재시도(네트워크 유실 후 동일 요청 재전송) → 멱등성 없을 때 문제 재현
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
    private lateinit var txManager: PlatformTransactionManager

    @Autowired
    private lateinit var redissonClient: RedissonClient

    @MockkBean
    private lateinit var outboxPublisher: OutboxPublisher

    @MockkBean
    private lateinit var gateway: PaymentGateway

    @BeforeEach
    fun setUp() {
        // 멱등성 캐시 초기화: 이전 테스트 실행에서 남은 Redis 키가 결과에 영향을 주지 않도록 한다
        redissonClient.keys.deleteByPattern("idempotency:*")
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()

        coEvery { gateway.approve(any()) } returns PgApproveResponse(
            pgTransactionId = "PG-test-${System.nanoTime()}",
            approvedAt = LocalDateTime.now(),
        )
    }

    /**
     * createPayment 재시도 문제 재현:
     * 1차 요청 성공 후 응답이 유실된 클라이언트가 동일 paymentKey로 재시도하면
     * 성공 응답 대신 DuplicatePayment 예외가 발생한다.
     * 클라이언트는 결제 실패로 오판 → 새 paymentKey로 재결제 → 이중과금 위험
     */
    @Test
    fun `createPayment 재시도 - 멱등성 없을 때 DuplicatePayment 오류 발생`() {
        val request = PaymentCreateRequest(
            paymentKey = "idempotency-test-create",
            merchantId = 1L,
            amount = BigDecimal("10000"),
            method = PaymentMethod.CARD,
        )

        paymentService.createPayment("user-1", request)

        // 문제: 서버는 이미 처리 완료했지만 클라이언트는 응답을 못 받은 상황(네트워크 유실)
        // 재시도 시 성공 응답이 아닌 예외가 발생 → 클라이언트는 결제 실패로 오판
        assertThrows<PaymentException.DuplicatePayment> {
            paymentService.createPayment("user-1", request)
        }
    }

    /**
     * approvePayment 재시도 문제 재현:
     * 승인 완료 후 응답이 유실된 클라이언트가 동일 paymentId로 재시도하면
     * 성공 응답 대신 InvalidStatus 예외가 발생한다.
     * PG에서는 이미 출금이 완료된 상태 → 클라이언트가 재결제를 시도하면 이중과금으로 이어진다.
     */
    @Test
    fun `approvePayment 재시도 - 멱등성 없을 때 InvalidStatus 오류 발생`() {
        val txTemplate = TransactionTemplate(txManager)

        val paymentId = txTemplate.execute {
            paymentRepository.save(
                PaymentEntity(
                    paymentKey = "idempotency-test-approve",
                    merchantId = 1L,
                    userId = "user-1",
                    amount = BigDecimal("50000"),
                    method = PaymentMethod.CARD,
                    status = PaymentStatus.READY,
                )
            ).id!!
        }!!

        paymentService.approvePayment(paymentId)

        // 문제: PG 출금까지 완료됐지만 클라이언트는 응답을 못 받은 상황(네트워크 유실)
        // 재시도 시 성공 응답이 아닌 예외가 발생 → 클라이언트는 승인 실패로 오판 → 재결제 → 이중과금
        assertThrows<PaymentException.InvalidStatus> {
            paymentService.approvePayment(paymentId)
        }
    }
}

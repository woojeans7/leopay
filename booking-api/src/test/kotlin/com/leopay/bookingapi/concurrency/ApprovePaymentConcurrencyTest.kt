package com.leopay.bookingapi.concurrency

import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.outbox.OutboxPublisher
import com.leopay.bookingapi.service.PaymentService
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.entity.PaymentHistoryEntity
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A-1 시나리오: 동일 paymentId에 동시 100req → 락 없을 때 중복 승인 재현, 락 있을 때 1건만 성공 검증
 *
 * develop test: 로컬 MySQL(localhost:3306) + Redis(localhost:6379) 실제 연결 필요
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = [KafkaAutoConfiguration::class])
@ActiveProfiles("test")
class ApprovePaymentConcurrencyTest {

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

    @MockkBean
    private lateinit var outboxPublisher: OutboxPublisher

    @MockkBean
    private lateinit var gateway: PaymentGateway

    @BeforeEach
    fun setUp() {
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()

        coEvery { gateway.approve(any()) } returns PgApproveResponse(
            pgTransactionId = "PG-test-${System.nanoTime()}",
            approvedAt = LocalDateTime.now(),
        )
    }

    /**
     * 락 없을 때 중복 승인 재현:
     * 락 없이 다수 스레드가 동시에 결제를 승인하면 중복이 발생함을 보여준다.
     * 각 스레드가 독립 트랜잭션으로 직접 repository를 호출해 "락 없는 환경"을 시뮬레이션.
     * assert 없이 로그로만 확인 — 테스트는 항상 통과한다.
     */
    @Test
    fun `동시 100req - 락 없을 때 중복 승인 발생`() {
        val txTemplate = TransactionTemplate(txManager)

        // READY 상태 결제 저장
        val paymentId = txTemplate.execute {
            paymentRepository.save(
                PaymentEntity(
                    paymentKey = "test-key-no-lock",
                    merchantId = 1L,
                    userId = "user-1",
                    amount = BigDecimal("10000"),
                    method = PaymentMethod.CARD,
                    status = PaymentStatus.READY,
                )
            ).id!!
        }!!

        val threadCount = 100
        val startLatch = CountDownLatch(1)
        val doneLatch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)

        // 락 없이 각 스레드가 독립 트랜잭션으로 상태 전이를 시도 → 동시성 문제 재현
        repeat(threadCount) {
            executor.submit {
                try {
                    startLatch.await()
                    txTemplate.execute {
                        val payment = paymentRepository.findById(paymentId)
                            .orElseThrow { IllegalStateException("not found") }

                        if (payment.status == PaymentStatus.READY) {
                            payment.transitionTo(PaymentStatus.IN_PROGRESS)
                            payment.transitionTo(PaymentStatus.APPROVED)
                            payment.pgTransactionId = "PG-${Thread.currentThread().threadId()}"
                            payment.approvedAt = LocalDateTime.now()

                            paymentHistoryRepository.save(
                                PaymentHistoryEntity(
                                    paymentId = paymentId,
                                    previousStatus = PaymentStatus.IN_PROGRESS,
                                    newStatus = PaymentStatus.APPROVED,
                                )
                            )
                            successCount.incrementAndGet()
                        } else {
                            failCount.incrementAndGet()
                        }
                    }
                } catch (e: Exception) {
                    failCount.incrementAndGet()
                } finally {
                    doneLatch.countDown()
                }
            }
        }

        startLatch.countDown()
        doneLatch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        val approvedCount = paymentHistoryRepository
            .findByPaymentIdAndNewStatus(paymentId, PaymentStatus.APPROVED)
            .size

        println("===== [A-1 락 없는 버전] =====")
        println("성공: ${successCount.get()}, 실패: ${failCount.get()}")
        println("중복 승인 발생 건수: $approvedCount")
        println("================================")
        // assert 없음 — 중복 발생 여부를 로그로 확인하는 용도
    }

    /**
     * 락 있을 때 1건만 성공:
     * PaymentService.approvePayment 는 RedisLockManager 를 통해 분산락을 획득한 후
     * TransactionTemplate 으로 새 트랜잭션을 시작한다 (lock이 transaction을 감싸는 구조).
     * 따라서 두 번째 스레드가 락을 획득할 때는 첫 번째 스레드의 커밋 결과(APPROVED)를 읽어
     * InvalidStatus 예외로 처리되고, APPROVED 이력은 정확히 1건이어야 한다.
     */
    @Test
    fun `동시 100req - 락 있을 때 1건만 성공`() {
        val txTemplate = TransactionTemplate(txManager)

        val paymentId = txTemplate.execute {
            paymentRepository.save(
                PaymentEntity(
                    paymentKey = "test-key-with-lock",
                    merchantId = 1L,
                    userId = "user-1",
                    amount = BigDecimal("10000"),
                    method = PaymentMethod.CARD,
                    status = PaymentStatus.READY,
                )
            ).id!!
        }!!

        val threadCount = 100
        val startLatch = CountDownLatch(1)
        val doneLatch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)

        repeat(threadCount) {
            executor.submit {
                try {
                    startLatch.await()
                    paymentService.approvePayment(paymentId)
                    successCount.incrementAndGet()
                } catch (e: Exception) {
                    failCount.incrementAndGet()
                } finally {
                    doneLatch.countDown()
                }
            }
        }

        startLatch.countDown()
        doneLatch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        val approvedCount = paymentHistoryRepository
            .findByPaymentIdAndNewStatus(paymentId, PaymentStatus.APPROVED)
            .size

        println("===== [A-1 락 있는 버전] =====")
        println("성공: ${successCount.get()}, 실패: ${failCount.get()}")
        println("중복 승인 발생 건수: $approvedCount")
        println("================================")

        assertEquals(1, approvedCount, "분산락 적용 시 APPROVED 이력은 정확히 1건이어야 합니다")
    }
}

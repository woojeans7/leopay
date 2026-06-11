package com.leopay.bookingapi.concurrency

import com.fasterxml.jackson.databind.ObjectMapper
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.idempotency.IdempotencyManager
import com.leopay.bookingapi.lock.LockManager
import com.leopay.bookingapi.lock.RedisLockManager
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
import io.mockk.every
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
    private lateinit var notificationRepository: NotificationRepository

    @Autowired
    private lateinit var settlementDetailRepository: SettlementDetailRepository

    @Autowired
    private lateinit var txManager: PlatformTransactionManager

    @Autowired
    private lateinit var redissonClient: RedissonClient

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @MockkBean
    private lateinit var outboxPublisher: OutboxPublisher

    @MockkBean
    private lateinit var gateway: PaymentGateway

    @MockkBean
    private lateinit var lockManager: LockManager

    @MockkBean
    private lateinit var idempotencyManager: IdempotencyManager

    /**
     * 실제 Redis 분산락 구현체 인스턴스.
     * @MockkBean이 컨텍스트에서 LockManager를 mock으로 교체하므로
     * 여기서 직접 생성해 락 있는 테스트의 withLock 위임에 사용한다.
     */
    private lateinit var realLockManager: RedisLockManager

    /**
     * 실제 멱등성 처리 구현체 인스턴스.
     * @MockkBean이 컨텍스트에서 IdempotencyManager를 mock으로 교체하므로
     * 여기서 직접 생성해 락 있는 테스트의 execute 위임에 사용한다.
     */
    private lateinit var realIdempotencyManager: IdempotencyManager

    @BeforeEach
    fun setUp() {
        realLockManager = RedisLockManager(redissonClient)
        realIdempotencyManager = IdempotencyManager(redissonClient, objectMapper)

        // 멱등성 캐시 초기화: 이전 테스트 실행에서 남은 Redis 키가 결과에 영향을 주지 않도록 한다
        redissonClient.keys.deleteByPattern("idempotency:*")
        settlementDetailRepository.deleteAll()
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()

        // OutboxPublisher는 Kafka 미연결 환경에서 예외를 던지지 않도록 no-op 처리
        every { outboxPublisher.publish() } returns Unit

        // gateway 기본 mock: 즉시 응답 (락 있는 테스트 기본값. 락 없는 테스트는 내부에서 delay 포함 mock으로 override)
        coEvery { gateway.approve(any()) } returns PgApproveResponse(
            pgTransactionId = "PG-default",
            approvedAt = LocalDateTime.now(),
        )

        // 기본값: 실제 Redis 분산락 위임 (락 있는 테스트용. 락 없는 테스트는 내부에서 no-op으로 override)
        every { lockManager.withLock(any<String>(), any<String>(), any<() -> Any>()) } answers {
            val prefix = firstArg<String>()
            val key = secondArg<String>()
            val block = thirdArg<() -> Any>()
            realLockManager.withLock(prefix, key, block)
        }
        // 기본값: 실제 멱등성 처리 위임 (락 있는 테스트용. 락 없는 테스트는 내부에서 no-op으로 override)
        every { idempotencyManager.execute(any(), any(), any(), any<Class<Any>>(), any<() -> Any>()) } answers {
            val userId = firstArg<String>()
            val apiName = secondArg<String>()
            val idempotencyKey = thirdArg<String>()
            @Suppress("UNCHECKED_CAST")
            val responseClass = arg<Class<Any>>(3)
            val block = lastArg<() -> Any>()
            realIdempotencyManager.execute(userId, apiName, idempotencyKey, responseClass, block)
        }
    }

    /**
     * 락 없을 때 중복 승인 재현:
     * 분산락과 멱등성 관리를 no-op으로 비활성화한 상태에서
     * 100개 스레드가 동시에 paymentService.approvePayment()를 호출한다.
     *
     * 재현 원리:
     *   - gateway.approve()에서 Thread.sleep(100ms)으로 PG 응답 지연 시뮬레이션
     *   - 100개 스레드가 동시에 READY → IN_PROGRESS 전이 시도
     *   - 락 없는 환경이므로 여러 스레드가 중복으로 READY 상태를 읽고 IN_PROGRESS로 전이 가능
     *   - APPROVED 이력이 2건 이상이면 중복 승인 재현 성공
     *
     * assert 없이 로그로만 확인 — 테스트는 항상 통과한다.
     */
    @Test
    fun `동시 100req - 락 없을 때 중복 승인 발생`() {
        // LockManager no-op: 락 획득 없이 즉시 블록 실행
        every { lockManager.withLock(any<String>(), any<String>(), any<() -> Any>()) } answers {
            val block = thirdArg<() -> Any>()
            block()
        }
        // IdempotencyManager no-op: 멱등성 체크 없이 즉시 블록 실행
        every { idempotencyManager.execute(any(), any(), any(), any<Class<Any>>(), any<() -> Any>()) } answers {
            val block = lastArg<() -> Any>()
            block()
        }
        // gateway: 100ms 지연 후 응답 — 이 딜레이 동안 여러 스레드가 중복으로 처리 진입 가능
        coEvery { gateway.approve(any()) } coAnswers {
            Thread.sleep(100L)
            PgApproveResponse(
                pgTransactionId = "PG-${Thread.currentThread().threadId()}",
                approvedAt = LocalDateTime.now(),
            )
        }

        val txTemplate = TransactionTemplate(txManager)

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

        repeat(threadCount) {
            executor.submit {
                try {
                    startLatch.await()
                    paymentService.approvePayment(paymentId)
                    successCount.incrementAndGet()
                } catch (e: Exception) {
                    val count = failCount.incrementAndGet()
                    if (count <= 3) println("[FAIL] ${e::class.simpleName}: ${e.message?.take(100)}")
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

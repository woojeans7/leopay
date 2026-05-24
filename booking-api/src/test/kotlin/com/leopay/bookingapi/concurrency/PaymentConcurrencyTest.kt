package com.leopay.bookingapi.concurrency

import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveRequest
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.gateway.dto.PgCancelRequest
import com.leopay.bookingapi.gateway.dto.PgCancelResponse
import com.leopay.bookingapi.service.PaymentService
import com.leopay.core.enums.PaymentMethod
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@SpringBootTest
@ActiveProfiles("test")
class PaymentConcurrencyTest {

    @TestConfiguration
    class MockGatewayConfig {
        @Bean
        @Primary
        fun mockPaymentGateway(): PaymentGateway = object : PaymentGateway {
            override fun approve(request: PgApproveRequest) = PgApproveResponse(
                success = true,
                transactionId = UUID.randomUUID().toString(),
                approvedAt = LocalDateTime.now(),
            )
            override fun cancel(request: PgCancelRequest) = PgCancelResponse(success = true)
        }
    }

    @Autowired lateinit var paymentService: PaymentService
    @Autowired lateinit var paymentRepository: PaymentRepository
    @Autowired lateinit var paymentHistoryRepository: PaymentHistoryRepository
    @Autowired lateinit var outboxEventRepository: OutboxEventRepository
    @Autowired lateinit var merchantRepository: MerchantRepository

    private var merchantId: Long = 0L
    private val threadCount = 100

    @BeforeEach
    fun setUp() {
        outboxEventRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        paymentRepository.deleteAll()

        merchantId = merchantRepository.findByBusinessNumber("123-45-67890")
            .orElseThrow { IllegalStateException("테스트용 가맹점이 없습니다. data.sql이 로드됐는지 확인하세요.") }
            .id!!
    }

    @Test
    @DisplayName("[락 있는 버전] 동시 100건 요청 → 중복 결제 0건")
    fun `with lock - 100 concurrent requests should create only 1 payment`() {
        val orderId = "ORDER-${UUID.randomUUID()}"
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)

        val executor = Executors.newFixedThreadPool(threadCount)
        val latch = CountDownLatch(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    paymentService.createPayment(
                        PaymentCreateRequest(
                            orderId = orderId,
                            merchantId = merchantId,
                            amount = BigDecimal("10000"),
                            method = PaymentMethod.CARD,
                        ),
                        userId = "user-1",
                    )
                    successCount.incrementAndGet()
                } catch (e: Exception) {
                    failCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }

        latch.await()
        executor.shutdown()

        val paymentCount = paymentRepository.findAll().count { it.orderId == orderId }

        println("=== [락 있는 버전] 결과 ===")
        println("성공: ${successCount.get()}건, 실패(락 충돌): ${failCount.get()}건")
        println("DB 저장된 payment 건수: $paymentCount")

        assertThat(paymentCount)
            .`as`("중복 결제가 발생하면 안 됩니다 (실제: ${paymentCount}건)")
            .isEqualTo(1)
    }
}

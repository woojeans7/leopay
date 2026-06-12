package com.leopay.bookingapi.outbox

import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.service.PaymentService
import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.OutboxEventEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.NotificationRepository
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.redisson.api.RLock
import org.redisson.api.RedissonClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.PageRequest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.util.concurrent.CompletableFuture

@SpringBootTest
@EnableAutoConfiguration(exclude = [KafkaAutoConfiguration::class])
@ActiveProfiles("test")
class OutboxPublisherTest {

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentRepository: PaymentRepository

    @Autowired
    private lateinit var outboxEventRepository: OutboxEventRepository

    @Autowired
    private lateinit var paymentHistoryRepository: PaymentHistoryRepository

    @Autowired
    private lateinit var settlementDetailRepository: SettlementDetailRepository

    @Autowired
    private lateinit var notificationRepository: NotificationRepository

    @Autowired
    private lateinit var merchantRepository: MerchantRepository

    private var merchantId: Long = 0L

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean
    private lateinit var redissonClient: RedissonClient

    @MockkBean
    private lateinit var gateway: PaymentGateway

    @SpykBean
    private lateinit var outboxPublisher: OutboxPublisher

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()
        merchantRepository.deleteAll()

        val merchant = merchantRepository.save(
            MerchantEntity(
                businessNumber = "1234567890",
                name = "테스트 가맹점",
                bankCode = "088",
                accountNumber = "123456789",
                accountHolder = "테스트",
                feeRate = java.math.BigDecimal("0.0300"),
                status = MerchantStatus.ACTIVE,
            )
        )
        merchantId = merchant.id!!

        val mockLock = mockk<RLock>()
        every { redissonClient.getLock(any<String>()) } returns mockLock
        every { mockLock.tryLock(any(), any(), any()) } returns true
        every { mockLock.isHeldByCurrentThread } returns true
        every { mockLock.unlock() } just Runs
    }

    @Test
    fun `Kafka 장애 시 결제와 Outbox 이벤트가 DB에 보존된다`() {
        every { kafkaTemplate.send(any<String>(), any<String>()) } throws RuntimeException("Kafka 연결 실패")

        val paymentKey = "outbox-kafka-failure-test"
        val request = PaymentCreateRequest(
            paymentKey = paymentKey,
            merchantId = merchantId,
            amount = BigDecimal("15000"),
            method = PaymentMethod.CARD,
        )

        paymentService.createPayment("user-1", request)

        assertTrue(paymentRepository.existsByPaymentKey(paymentKey), "결제 레코드가 DB에 저장되어야 한다")

        val unpublishedEvents = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc(
            PageRequest.of(0, 10)
        )
        assertEquals(1, unpublishedEvents.size, "미발행 Outbox 이벤트가 1건 존재해야 한다")
        assertFalse(unpublishedEvents[0].published, "이벤트는 published=false 상태를 유지해야 한다")
    }

    @Test
    fun `Kafka 장애 후 publish 호출 시 이벤트가 미발행 상태로 유지된다`() {
        every { kafkaTemplate.send(any<String>(), any<String>()) } throws RuntimeException("Kafka 연결 실패")

        val paymentKey = "outbox-publish-failure-test"
        val request = PaymentCreateRequest(
            paymentKey = paymentKey,
            merchantId = merchantId,
            amount = BigDecimal("20000"),
            method = PaymentMethod.CARD,
        )

        paymentService.createPayment("user-1", request)

        try {
            outboxPublisher.publish()
        } catch (_: Exception) {
        }

        val unpublishedEvents = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc(
            PageRequest.of(0, 10)
        )
        assertEquals(1, unpublishedEvents.size, "Kafka 실패 후 이벤트는 published=false를 유지해야 한다")
        assertFalse(unpublishedEvents[0].published, "이벤트는 published=false 상태를 유지해야 한다")
    }

    @Test
    fun `Kafka 복구 후 Scheduler가 미발행 이벤트를 재발행한다`() {
        every { kafkaTemplate.send(any<String>(), any<String>()) } returns CompletableFuture.completedFuture(null)

        outboxEventRepository.save(
            OutboxEventEntity(
                aggregateType = "PAYMENT",
                aggregateId = 999L,
                eventType = "PAYMENT_CREATED",
                payload = """{"paymentId":999}""",
                published = false,
            )
        )

        outboxPublisher.publish()

        val savedEvent = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc(PageRequest.of(0, 10))
        assertTrue(savedEvent.isEmpty(), "publish 이후 미발행 이벤트가 남아있으면 안 된다")

        val allEvents = outboxEventRepository.findAll()
        assertEquals(1, allEvents.size)
        assertTrue(allEvents[0].published, "이벤트가 published=true로 업데이트되어야 한다")
        assertNotNull(allEvents[0].publishedAt, "publishedAt이 설정되어야 한다")

        verify(exactly = 1) { kafkaTemplate.send(any<String>(), any<String>()) }
    }
}


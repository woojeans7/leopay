package com.leopay.notificationworker.consumer

import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.NotificationRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.util.concurrent.TimeUnit

/**
 * B-3 시나리오 검증: 컨슈머 중복 소비 멱등성
 *
 * 문제: Kafka at-least-once → 같은 메시지가 2회 수신될 수 있음 → 알림 중복 발송
 * 해결: paymentId + NotificationType 조합 → 이미 SENT 처리된 경우 skip
 *
 * 검증:
 *   1. 같은 paymentId 메시지를 2회 전송
 *   2. notification 레코드가 1건만 생성됨을 확인 (중복 저장 안 됨)
 *
 * develop test: MySQL(localhost:3306) + EmbeddedKafka 사용
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = [
        "payment.approved",
        "payment.canceled",
        "payment.approved.DLT",
        "payment.canceled.DLT",
    ],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
)
@ActiveProfiles("test")
class NotificationIdempotencyTest {

    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var notificationRepository: NotificationRepository
    @Autowired private lateinit var paymentRepository: PaymentRepository
    @Autowired private lateinit var paymentHistoryRepository: PaymentHistoryRepository

    private var testPaymentId: Long = 0L

    @BeforeEach
    fun setUp() {
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        paymentRepository.deleteAll()
        testPaymentId = paymentRepository.save(
            PaymentEntity(
                paymentKey = "idempotency-test-${System.nanoTime()}",
                merchantId = 1L,
                userId = "user-1",
                amount = BigDecimal("10000"),
                method = PaymentMethod.CARD,
                status = PaymentStatus.APPROVED,
            )
        ).id!!
    }

    @Test
    fun `payment_approved 동일 메시지 2회 수신 시 notification 1건만 저장`() {
        val payload = """{"paymentId":$testPaymentId}"""

        // 첫 번째 메시지 전송 후 처리 대기
        kafkaTemplate.send("payment.approved", payload)
        waitForNotificationCount(expectedCount = 1, timeoutSeconds = 10)

        assertThat(notificationRepository.count())
            .`as`("첫 번째 메시지 처리 후 notification 이 1건 저장되어야 한다")
            .isEqualTo(1L)

        // 두 번째 메시지 전송 (중복 수신 시뮬레이션)
        kafkaTemplate.send("payment.approved", payload)

        // 중복 skip 대기: 두 번째 메시지가 소비될 시간 + 여유
        TimeUnit.SECONDS.sleep(2)

        // 핵심 검증: 중복 소비 skip → notification 은 여전히 1건
        assertThat(notificationRepository.count())
            .`as`("중복 메시지 skip → notification 은 1건이어야 한다 (중복 저장 없음)")
            .isEqualTo(1L)
    }

    @Test
    fun `payment_canceled 동일 메시지 2회 수신 시 notification 1건만 저장`() {
        val payload = """{"paymentId":$testPaymentId}"""

        // 첫 번째 메시지 전송 후 처리 대기
        kafkaTemplate.send("payment.canceled", payload)
        waitForNotificationCount(expectedCount = 1, timeoutSeconds = 10)

        assertThat(notificationRepository.count())
            .`as`("첫 번째 메시지 처리 후 notification 이 1건 저장되어야 한다")
            .isEqualTo(1L)

        // 두 번째 메시지 전송 (중복 수신 시뮬레이션)
        kafkaTemplate.send("payment.canceled", payload)

        TimeUnit.SECONDS.sleep(2)

        assertThat(notificationRepository.count())
            .`as`("중복 메시지 skip → notification 은 1건이어야 한다 (중복 저장 없음)")
            .isEqualTo(1L)
    }

    /** notification 레코드 수가 expectedCount 에 도달할 때까지 polling */
    private fun waitForNotificationCount(expectedCount: Long, timeoutSeconds: Long) {
        val deadline = System.currentTimeMillis() + timeoutSeconds * 1_000L
        while (System.currentTimeMillis() < deadline) {
            if (notificationRepository.count() >= expectedCount) return
            TimeUnit.MILLISECONDS.sleep(200)
        }
    }
}

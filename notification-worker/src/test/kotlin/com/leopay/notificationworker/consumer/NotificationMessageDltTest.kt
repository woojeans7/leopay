package com.leopay.notificationworker.consumer

import com.leopay.core.enums.NotificationStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.notificationworker.service.NotificationService
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.NotificationRepository
import com.leopay.storage.repository.PaymentRepository
import com.ninjasquad.springmockk.SpykBean
import io.mockk.every
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A-4 2단계 검증: DefaultErrorHandler(3회 재시도) + DLT 이동 확인
 *
 * 검증 항목:
 *   1. 컨슈머가 정확히 4회 호출됨 (1회 원본 처리 + 3회 재시도)
 *   2. DLT 토픽(payment.approved.DLT)에 메시지 도착 → DltNotificationConsumer 가 수신
 *   3. notification 테이블에 FAILED 레코드 1건 존재 (DLT 컨슈머가 저장한 이력)
 *
 * 설정:
 *   - KafkaConsumerConfig 가 등록한 DefaultErrorHandler(FixedBackOff 1초 × 3회)를 그대로 사용
 *   - NotificationMessageLossTest 의 FastErrorHandlerConfig 와 충돌하지 않도록
 *     이 테스트는 @TestConfiguration 을 별도 선언하지 않는다
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
class NotificationMessageDltTest {

    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var notificationRepository: NotificationRepository
    @Autowired private lateinit var paymentRepository: PaymentRepository
    @Autowired private lateinit var paymentHistoryRepository: com.leopay.storage.repository.PaymentHistoryRepository

    @SpykBean private lateinit var notificationService: NotificationService

    private var testPaymentId: Long = 0L

    @BeforeEach
    fun setUp() {
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        paymentRepository.deleteAll()
        testPaymentId = paymentRepository.save(
            PaymentEntity(
                paymentKey = "dlt-test-${System.nanoTime()}",
                merchantId = 1L,
                userId = "user-1",
                amount = BigDecimal("10000"),
                method = PaymentMethod.CARD,
                status = PaymentStatus.READY,
            )
        ).id!!
    }

    /**
     * 검증 1·2·3 통합:
     *   NotificationService 가 항상 예외를 던지도록 설정
     *   → KafkaConsumerConfig 에러 핸들러가 1초 간격 3회 재시도 (총 4회 호출)
     *   → 재시도 소진 후 payment.approved.DLT 로 이동
     *   → DltNotificationConsumer 가 FAILED 알림 이력 저장
     */
    @Test
    fun `3회 재시도 후 DLT 이동 - FAILED 이력 저장 확인`() {
        // 총 4회 호출(1회+재시도3회)을 기다리기 위한 래치
        val invocationLatch = CountDownLatch(4)

        every { notificationService.sendApprovedNotification(any()) } answers {
            invocationLatch.countDown()
            throw RuntimeException("외부 알림 서비스 연결 실패 (시뮬레이션)")
        }

        kafkaTemplate.send("payment.approved", """{"paymentId":$testPaymentId}""")

        // 검증 1: 컨슈머가 정확히 4회 호출됨 (1회 + 3회 재시도)
        // FixedBackOff(1000L, 3L) → 최대 3초 대기 + 여유 2초 = 5초
        val invokedFourTimes = invocationLatch.await(10, TimeUnit.SECONDS)
        assertThat(invokedFourTimes)
            .`as`("컨슈머가 10초 내에 정확히 4회(1회+재시도3회) 호출되어야 한다")
            .isTrue()

        // 검증 2·3: DLT 이동 후 DltNotificationConsumer 가 FAILED 저장 완료 대기
        // DLT 메시지 발행 → DLT 컨슈머 소비 → DB 저장 경로이므로 약간의 시간 필요
        val deadline = System.currentTimeMillis() + 10_000L
        var failedCount = 0L
        while (System.currentTimeMillis() < deadline) {
            failedCount = notificationRepository.countByStatus(NotificationStatus.FAILED)
            if (failedCount >= 1L) break
            Thread.sleep(200)
        }

        assertThat(failedCount)
            .`as`("DLT 컨슈머가 FAILED 알림 이력을 1건 저장해야 한다")
            .isEqualTo(1L)

        val failedNotification = notificationRepository.findAll()
            .first { it.status == NotificationStatus.FAILED }

        assertThat(failedNotification.paymentId)
            .`as`("저장된 FAILED 이력의 paymentId 가 일치해야 한다")
            .isEqualTo(testPaymentId)

        assertThat(failedNotification.failureReason)
            .`as`("failureReason 이 기록되어야 한다")
            .isNotBlank()
    }
}

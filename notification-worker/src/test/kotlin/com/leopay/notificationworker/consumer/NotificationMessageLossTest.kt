package com.leopay.notificationworker.consumer

import com.leopay.notificationworker.service.NotificationService
import com.leopay.storage.repository.NotificationRepository
import com.ninjasquad.springmockk.SpykBean
import io.mockk.every
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.backoff.FixedBackOff
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A-4 시나리오 1단계(문제 재현): 에러 핸들러/DLT 미설정 시 메시지 유실 확인
 *
 * 문제: 컨슈머 처리 중 예외 발생
 *      → 재시도 후 offset commit → 메시지 영구 유실 → 알림 미발송
 *
 * 재현 방법:
 *   1. NotificationService 예외 주입 (외부 알림 서비스 장애 시뮬레이션)
 *   2. 메시지 전송 → 컨슈머 처리 시도 → 예외 발생
 *   3. notification 레코드 0건 확인 → 알림이 영구 미발송 상태임을 증명
 *
 * 해결: NotificationMessageDltTest (2단계) — DefaultErrorHandler(3회) + DLT 적용 후 메시지 보존 확인
 *
 * develop test: MySQL(localhost:3306) + EmbeddedKafka 사용
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = ["payment.approved", "payment.canceled"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
)
@ActiveProfiles("test")
class NotificationMessageLossTest {

    /**
     * 테스트 전용 KafkaListenerContainerFactory:
     * 재시도 0회로 설정해 기본 9회 재시도 대기 없이 즉시 메시지 유실 동작을 재현한다.
     * (운영 코드에는 에러 핸들러 미설정 → Spring Kafka 기본값 적용)
     */
    @TestConfiguration
    class FastErrorHandlerConfig {
        @Bean
        fun kafkaListenerContainerFactory(
            consumerFactory: ConsumerFactory<String, String>,
        ): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
            factory.consumerFactory = consumerFactory
            factory.setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(0L, 0L)))
            return factory
        }
    }

    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var notificationRepository: NotificationRepository
    @SpykBean private lateinit var notificationService: NotificationService

    @BeforeEach
    fun setUp() {
        notificationRepository.deleteAll()
    }

    @Test
    fun `처리 실패 시 메시지 유실 - 알림 미발송 재현`() {
        val latch = CountDownLatch(1)

        every { notificationService.sendApprovedNotification(any()) } answers {
            latch.countDown()
            throw RuntimeException("외부 알림 서비스 연결 실패 (시뮬레이션)")
        }

        kafkaTemplate.send("payment.approved", """{"paymentId":1}""")

        assertThat(latch.await(5, TimeUnit.SECONDS))
            .`as`("컨슈머가 5초 내에 메시지를 처리 시도해야 한다")
            .isTrue()

        Thread.sleep(200)

        // 메시지 유실 확인: 예외 발생으로 트랜잭션 롤백 → notification 레코드 0건
        // DLT 없음 → 메시지 추적/재처리 불가 → 알림 영구 미발송
        assertThat(notificationRepository.count())
            .`as`("에러 핸들러/DLT 미설정 시 알림 레코드가 생성되지 않아야 한다 (메시지 유실)")
            .isEqualTo(0)
    }
}

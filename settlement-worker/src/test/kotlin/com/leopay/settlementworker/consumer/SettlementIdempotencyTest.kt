package com.leopay.settlementworker.consumer

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.settlementworker.scheduler.SettlementScheduler
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.ninjasquad.springmockk.MockkBean
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * 컨슈머 중복 소비 멱등성 검증
 *
 * 문제: Kafka at-least-once 보장 → 동일 메시지가 2회 이상 수신될 수 있음 → settlement_detail 중복 적재
 * 해결: SettlementDetailService.existsByPaymentId() 중복 체크 → 이미 적재된 paymentId 는 skip
 *       DataIntegrityViolationException(unique constraint) 은 consumer 레벨에서 catch → DLT 미발동
 *
 * 검증:
 *   1. 동일 paymentId 메시지 2회 전송
 *   2. settlement_detail 레코드가 1건만 생성됨을 확인 (중복 저장 없음)
 *
 * develop test: MySQL(localhost:3306) + EmbeddedKafka 사용
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = [
        "payment.approved",
        "payment.approved.DLT",
    ],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
)
@ActiveProfiles("test")
class SettlementIdempotencyTest {

    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var paymentRepository: PaymentRepository
    @Autowired private lateinit var merchantRepository: MerchantRepository
    @Autowired private lateinit var settlementDetailRepository: SettlementDetailRepository

    // RedissonClient Bean 초기화 시 Redis 연결 시도를 막는다.
    @MockkBean private lateinit var redissonClient: org.redisson.api.RedissonClient

    // @Scheduled 자정 실행으로 인한 부작용을 방지한다.
    @MockkBean private lateinit var settlementScheduler: SettlementScheduler

    private val settlementDate: LocalDate = LocalDate.now().minusDays(1)
    private val feeRate = BigDecimal("0.0350")
    private val paymentAmount = BigDecimal("50000")

    private var testPaymentId: Long = 0L

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()

        val merchantId = merchantRepository.save(
            MerchantEntity(
                businessNumber = "IDMP-${UUID.randomUUID().toString().take(8)}",
                name = "멱등성 테스트 가맹점",
                bankCode = "004",
                accountNumber = "123-456-789012",
                accountHolder = "테스트",
                feeRate = feeRate,
                status = MerchantStatus.ACTIVE,
            )
        ).id!!

        testPaymentId = paymentRepository.save(
            PaymentEntity(
                paymentKey = "IDMP-${UUID.randomUUID().toString().take(12)}",
                merchantId = merchantId,
                userId = "user-idmp",
                amount = paymentAmount,
                method = PaymentMethod.CARD,
                status = PaymentStatus.APPROVED,
                pgTransactionId = "PG-IDMP-001",
                approvedAt = settlementDate.atStartOfDay().plusHours(10),
            )
        ).id!!
    }

    @Test
    fun `payment_approved 동일 메시지 2회 수신 시 settlement_detail 1건만 저장`() {
        val payload = """{"paymentId":$testPaymentId}"""

        // 첫 번째 메시지 전송 후 처리 대기
        kafkaTemplate.send("payment.approved", payload)
        waitForSettlementDetail(expectedCount = 1, timeoutSeconds = 10)

        assertThat(settlementDetailRepository.count())
            .`as`("첫 번째 메시지 처리 후 settlement_detail 이 1건 저장되어야 한다")
            .isEqualTo(1L)

        // 두 번째 메시지 전송 (중복 수신 시뮬레이션)
        kafkaTemplate.send("payment.approved", payload)

        // 두 번째 메시지가 소비될 시간 확보
        TimeUnit.SECONDS.sleep(2)

        // 핵심 검증: 중복 소비 skip → settlement_detail 은 여전히 1건
        assertThat(settlementDetailRepository.count())
            .`as`("중복 메시지 skip → settlement_detail 은 1건이어야 한다 (중복 저장 없음)")
            .isEqualTo(1L)

        assertThat(settlementDetailRepository.existsByPaymentId(testPaymentId))
            .`as`("paymentId 로 조회 시 1건이 존재해야 한다")
            .isTrue()
    }

    /** settlement_detail 레코드 수가 expectedCount 에 도달할 때까지 polling */
    private fun waitForSettlementDetail(expectedCount: Long, timeoutSeconds: Long) {
        val deadline = System.currentTimeMillis() + timeoutSeconds * 1_000L
        while (System.currentTimeMillis() < deadline) {
            if (settlementDetailRepository.count() >= expectedCount) return
            TimeUnit.MILLISECONDS.sleep(200)
        }
    }
}

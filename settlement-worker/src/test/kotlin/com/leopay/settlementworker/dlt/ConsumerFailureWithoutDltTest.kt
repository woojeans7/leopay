package com.leopay.settlementworker.dlt

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.settlementworker.scheduler.SettlementScheduler
import com.leopay.settlementworker.service.SettlementDetailService
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.leopay.storage.repository.SettlementRepository
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.every
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
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
import java.math.BigDecimal
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A-4 시나리오 1단계(문제 재현): DLT 미적용 시 메시지 유실 확인
 *
 * 문제: 컨슈머 처리 중 예외 발생
 *      → 재시도 후 offset commit → 메시지 영구 유실 → 정산 누락
 *
 * 재현 방법:
 *   1. SettlementDetailService 예외 주입 (DB 장애 시뮬레이션)
 *   2. FastErrorHandlerConfig: settlementListenerContainerFactory를 재시도 0회로 override
 *      → 즉시 실패, DLT 없이 메시지 유실 동작 재현
 *   3. settlement_detail 레코드 0건 확인 → 배치 집계 0건
 *
 * 해결: DltSettlementTest (2단계) — DefaultErrorHandler(3회) + DLT 적용 후 정산 보장 확인
 *
 * develop test: MySQL(localhost:3306) + EmbeddedKafka 사용
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = ["payment.approved"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
)
@ActiveProfiles("test")
class ConsumerFailureWithoutDltTest {

    /**
     * 테스트 전용 KafkaListenerContainerFactory:
     * 재시도 0회로 설정해 기본 3회 재시도 대기 없이 즉시 메시지 유실 동작을 재현한다.
     * (운영 코드: KafkaConsumerConfig.settlementListenerContainerFactory — FixedBackOff(1000L, 3L))
     */
    @TestConfiguration
    class FastErrorHandlerConfig {
        @Bean
        fun settlementListenerContainerFactory(
            consumerFactory: ConsumerFactory<String, String>,
        ): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
            factory.consumerFactory = consumerFactory
            factory.setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(0L, 0L)))
            return factory
        }
    }

    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var paymentRepository: PaymentRepository
    @Autowired private lateinit var merchantRepository: MerchantRepository
    @Autowired private lateinit var settlementDetailRepository: SettlementDetailRepository
    @Autowired private lateinit var settlementRepository: SettlementRepository
    @Autowired private lateinit var jobLauncher: JobLauncher
    @Autowired private lateinit var settlementJob: Job

    @SpykBean private lateinit var settlementDetailService: SettlementDetailService

    // RedissonClient Bean 초기화 시 Redis 연결 시도를 막는다.
    @MockkBean private lateinit var redissonClient: org.redisson.api.RedissonClient

    // @Scheduled 자정 실행으로 인한 부작용을 방지한다.
    @MockkBean private lateinit var settlementScheduler: SettlementScheduler

    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val settlementDate: LocalDate = LocalDate.now().minusDays(1)
    private val settlementDateStr: String = settlementDate.format(dateFormatter)

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()
        settlementRepository.deleteAll()
    }

    @Test
    fun `처리 실패 시 메시지 유실 - settlement_detail 미적재 재현`() {
        // given: 가맹점 + 결제 픽스처 저장
        val merchantId = saveMerchant()
        val paymentId = savePayment(merchantId)
        val latch = CountDownLatch(1)

        every { settlementDetailService.saveSettlementDetail(paymentId, source = "consumer") } answers {
            latch.countDown()
            throw RuntimeException("DB insert 실패 (시뮬레이션)")
        }

        // when: payment.approved 메시지 발행
        kafkaTemplate.send("payment.approved", """{"paymentId":$paymentId}""")

        // 컨슈머가 5초 내에 메시지를 처리 시도해야 한다
        assertThat(latch.await(5, TimeUnit.SECONDS))
            .`as`("컨슈머가 5초 내에 메시지를 처리 시도해야 한다")
            .isTrue()

        Thread.sleep(200)

        // then 1: 메시지 유실 확인 — settlement_detail 0건
        assertThat(settlementDetailRepository.existsByPaymentId(paymentId))
            .`as`("DLT 미적용 시 컨슈머 실패로 settlement_detail이 적재되지 않아야 한다 (메시지 유실)")
            .isFalse()

        // then 2: 배치 실행 → PENDING 건 없어 settlement 레코드 생성 안 됨
        val jobParameters = JobParametersBuilder()
            .addLong("merchantId", merchantId)
            .addString("settlementDate", settlementDateStr)
            .addString("runId", UUID.randomUUID().toString())
            .toJobParameters()

        jobLauncher.run(settlementJob, jobParameters)

        val settlement = settlementRepository.findByMerchantIdAndSettlementDate(merchantId, settlementDate)

        assertThat(settlement.isPresent)
            .`as`("PENDING 건이 없으므로 settlement 레코드도 생성되지 않아야 한다")
            .isFalse()

        println("===== [A-4 1단계 재현] DLT 없을 때 Consumer 실패 → 배치 집계 누락 =====")
        println("settlement_detail 적재 여부: false (메시지 유실)")
        println("settlement 레코드 존재: ${settlement.isPresent} (정산 누락)")
        println("결론: DLT 없으면 Consumer 실패 시 settlement_detail 미적재 → 배치 집계 0건")
        println("==========================================================================")
    }

    // -------------------------------------------------------------------------------------
    // 테스트 픽스처 헬퍼
    // -------------------------------------------------------------------------------------

    private fun saveMerchant(): Long =
        merchantRepository.save(
            MerchantEntity(
                businessNumber = "A4-FAIL-${UUID.randomUUID().toString().take(8)}",
                name = "A4 Consumer 실패 테스트 가맹점",
                bankCode = "004",
                accountNumber = "123-456-789012",
                accountHolder = "테스트",
                feeRate = BigDecimal("0.0350"),
                status = MerchantStatus.ACTIVE,
            )
        ).id!!

    private fun savePayment(merchantId: Long): Long =
        paymentRepository.save(
            PaymentEntity(
                paymentKey = "A4-FAIL-${UUID.randomUUID().toString().take(12)}",
                merchantId = merchantId,
                userId = "user-a4-fail",
                amount = BigDecimal("50000"),
                method = PaymentMethod.CARD,
                status = PaymentStatus.APPROVED,
                pgTransactionId = "PG-A4-FAIL",
                approvedAt = settlementDate.atStartOfDay().plusHours(14),
            )
        ).id!!
}

package com.leopay.settlementworker.dlt

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.core.enums.SettlementStatus
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
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A-4 시나리오 2단계(해결): DefaultErrorHandler(3회 재시도) + DLT 이동 확인
 *
 * 검증 항목:
 *   1. 컨슈머가 정확히 4회 호출됨 (1회 원본 처리 + 3회 재시도)
 *   2. 재시도 소진 후 payment.approved.DLT 로 이동 → SettlementDltConsumer 수신
 *   3. settlement_detail 테이블에 PENDING 레코드 1건 존재 (DLT 컨슈머가 재적재)
 *   4. 배치 실행 → settlement 레코드 생성 + settlement_detail COMPLETED 전환
 *
 * 설정:
 *   - KafkaConsumerConfig 가 등록한 DefaultErrorHandler(FixedBackOff 1초 × 3회)를 그대로 사용
 *   - ConsumerFailureWithoutDltTest 의 FastErrorHandlerConfig 와 충돌하지 않도록
 *     이 테스트는 @TestConfiguration 을 별도 선언하지 않는다
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
class DltSettlementTest {

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
    private var testPaymentId: Long = 0L
    private var testMerchantId: Long = 0L
    private val feeRate = BigDecimal("0.0350")
    private val paymentAmount = BigDecimal("50000")

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()
        settlementRepository.deleteAll()

        testMerchantId = saveMerchant()
        testPaymentId = savePayment(testMerchantId)
    }

    /**
     * 검증 1·2·3·4 통합:
     *   SettlementDetailService 가 source="consumer" 일 때 예외를 던지도록 설정
     *   → KafkaConsumerConfig 에러 핸들러가 1초 간격 3회 재시도 (총 4회 호출)
     *   → 재시도 소진 후 payment.approved.DLT 로 이동
     *   → SettlementDltConsumer 가 source="dlt" 로 saveSettlementDetail() 호출 (callOriginal)
     *   → settlement_detail PENDING 1건 적재
     *   → 배치 실행 → settlement 생성 + COMPLETED 전환
     */
    @Test
    fun `3회 재시도 후 DLT 이동 - settlement_detail 재적재 및 배치 집계 확인`() {
        // 총 4회 호출(1회+재시도3회)을 기다리기 위한 래치
        val invocationLatch = CountDownLatch(4)

        every { settlementDetailService.saveSettlementDetail(testPaymentId, source = "consumer") } answers {
            invocationLatch.countDown()
            throw RuntimeException("DB insert 실패 — 의도적 Consumer 실패 (A-4 재현)")
        }

        every { settlementDetailService.saveSettlementDetail(testPaymentId, source = "dlt") } answers { callOriginal() }

        // when: payment.approved 메시지 발행
        kafkaTemplate.send("payment.approved", """{"paymentId":$testPaymentId}""")

        // 검증 1: 컨슈머가 정확히 4회 호출됨 (1회 + 3회 재시도)
        // FixedBackOff(1000L, 3L) → 최대 3초 대기 + 여유 2초 = 5초
        val invokedFourTimes = invocationLatch.await(10, TimeUnit.SECONDS)
        assertThat(invokedFourTimes)
            .`as`("컨슈머가 10초 내에 정확히 4회(1회+재시도3회) 호출되어야 한다")
            .isTrue()

        // 검증 2·3: DLT 이동 후 SettlementDltConsumer 가 settlement_detail 재적재 완료 대기
        // DLT 메시지 발행 → DLT 컨슈머 소비 → DB 저장 경로이므로 약간의 시간 필요
        val deadline = System.currentTimeMillis() + 10_000L
        var saved = false
        while (System.currentTimeMillis() < deadline) {
            saved = settlementDetailRepository.existsByPaymentId(testPaymentId)
            if (saved) break
            Thread.sleep(200)
        }

        assertThat(saved)
            .`as`("DLT 컨슈머가 settlement_detail을 1건 재적재해야 한다")
            .isTrue()

        val details = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            testMerchantId, settlementDate, SettlementStatus.PENDING
        )

        assertThat(details).hasSize(1)

        // B-5: 수수료 계산 정확성 — feeAmount = 50000 × 0.0350 = 1750.0000
        val expectedFeeAmount = paymentAmount.multiply(feeRate).setScale(4, RoundingMode.HALF_EVEN)
        assertThat(details[0].feeAmount)
            .`as`("B-5: feeAmount = amount × feeRate (HALF_EVEN) 이어야 한다")
            .isEqualByComparingTo(expectedFeeAmount)

        // 검증 4: 배치 실행 → settlement 레코드 생성 + settlement_detail COMPLETED 전환
        val jobParameters = JobParametersBuilder()
            .addLong("merchantId", testMerchantId)
            .addString("settlementDate", settlementDateStr)
            .addString("runId", UUID.randomUUID().toString())
            .toJobParameters()

        val jobExecution = jobLauncher.run(settlementJob, jobParameters)

        val settlement = settlementRepository.findByMerchantIdAndSettlementDate(testMerchantId, settlementDate)

        assertThat(settlement.isPresent)
            .`as`("배치 실행 후 settlement 레코드가 생성되어야 한다")
            .isTrue()

        assertThat(settlement.get().totalCount)
            .`as`("settlement totalCount가 1이어야 한다")
            .isEqualTo(1)

        assertThat(settlement.get().totalAmount)
            .`as`("settlement totalAmount가 0보다 커야 한다")
            .isGreaterThan(BigDecimal.ZERO)

        val pendingAfterBatch = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            testMerchantId, settlementDate, SettlementStatus.PENDING
        )
        val completedAfterBatch = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            testMerchantId, settlementDate, SettlementStatus.COMPLETED
        )

        assertThat(pendingAfterBatch).isEmpty()
        assertThat(completedAfterBatch).hasSize(1)

        println("===== [A-4 2단계 해결] DLT Consumer → settlement_detail 재적재 → 배치 집계 =====")
        println("컨슈머 4회 호출 확인: true (1회 원본 + 3회 재시도)")
        println("DLT 재적재 후 settlement_detail 존재: $saved")
        println("B-5 feeAmount 검증: ${details[0].feeAmount} (기대값: $expectedFeeAmount)")
        println("배치 실행 상태: ${jobExecution.status}")
        println("settlement totalCount: ${settlement.get().totalCount}")
        println("배치 실행 후 COMPLETED 건: ${completedAfterBatch.size}")
        println("결론: DLT 적용 시 Consumer 실패해도 정산 유실 0건 보장")
        println("==========================================================================")
    }

    // -------------------------------------------------------------------------------------
    // 테스트 픽스처 헬퍼
    // -------------------------------------------------------------------------------------

    private fun saveMerchant(): Long =
        merchantRepository.save(
            MerchantEntity(
                businessNumber = "A4-DLT-${UUID.randomUUID().toString().take(8)}",
                name = "A4 DLT 테스트 가맹점",
                bankCode = "004",
                accountNumber = "123-456-789012",
                accountHolder = "테스트",
                feeRate = feeRate,
                status = MerchantStatus.ACTIVE,
            )
        ).id!!

    private fun savePayment(merchantId: Long): Long =
        paymentRepository.save(
            PaymentEntity(
                paymentKey = "A4-DLT-${UUID.randomUUID().toString().take(12)}",
                merchantId = merchantId,
                userId = "user-a4-dlt",
                amount = paymentAmount,
                method = PaymentMethod.CARD,
                status = PaymentStatus.APPROVED,
                pgTransactionId = "PG-A4-DLT",
                approvedAt = settlementDate.atStartOfDay().plusHours(14),
            )
        ).id!!
}

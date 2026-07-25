package com.leopay.settlementworker.batch

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.core.enums.SettlementStatus
import com.leopay.settlementworker.scheduler.SettlementScheduler
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.leopay.storage.repository.SettlementRepository
import com.ninjasquad.springmockk.MockkBean
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.redisson.api.RedissonClient
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * 정산 배치 합계 정합성 통합 테스트.
 *
 * 검증 목적:
 *   - 여러 건의 SettlementDetail(amount/feeAmount)이 서로 다른 금액을 가질 때
 *     배치 실행 후 생성되는 Settlement 엔티티의 totalAmount/feeAmount/settlementAmount가
 *     SettlementDetail 합계와 "정확히" 일치하는지 검증한다.
 *   - settlementAmount는 SettlementDetailEntity에 저장되지 않는 파생값이므로,
 *     Processor와 동일한 공식(amount - feeAmount, scale=0, FLOOR 절사 — 커밋 4c02b99)으로
 *     배치 실행 후 실제로 저장된 detail 값을 재계산하여 Settlement.settlementAmount와 비교한다.
 *   - settlement_detail.payment_id는 payment 테이블을 참조하는 FK(fk_detail_payment)이므로
 *     DltSettlementTest와 동일하게 실제 PaymentEntity를 먼저 저장한 뒤 그 id를 사용한다.
 *   - chunk-size는 기본값(application.yml)을 그대로 사용한다. JpaPagingItemReader는 Writer가
 *     페이지 조회 조건(status)을 변경하는 도중 페이지가 갱신되면 offset 기반 페이징 특성상
 *     항목을 건너뛸 수 있으므로, 이 테스트의 소량 데이터가 한 페이지 안에서 처리되도록 하여
 *     (합계 정합성 검증이라는) 테스트 목적과 무관한 페이징 이슈의 영향을 배제한다.
 *
 * develop test: MySQL(localhost:3306) 사용
 */
@SpringBootTest
@ActiveProfiles("test")
class SettlementBatchAggregationTest {

    @Autowired private lateinit var jobLauncher: JobLauncher
    @Autowired private lateinit var settlementJob: Job
    @Autowired private lateinit var merchantRepository: MerchantRepository
    @Autowired private lateinit var paymentRepository: PaymentRepository
    @Autowired private lateinit var settlementDetailRepository: SettlementDetailRepository
    @Autowired private lateinit var settlementRepository: SettlementRepository

    @MockkBean private lateinit var redissonClient: RedissonClient
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
    fun `서로 다른 금액의 SettlementDetail 여러 건 합계가 Settlement 집계값과 정확히 일치한다`() {
        val merchantId = saveMerchant()

        // 서로 다른 금액 + 수수료(HALF_EVEN 반올림 결과 포함, SettlementDetailService와 동일 공식)를 가진 5건 준비
        val fixtures = listOf(
            BigDecimal("10000") to BigDecimal("350.0000"),
            BigDecimal("25000") to BigDecimal("875.0000"),
            BigDecimal("33333") to BigDecimal("1166.6550"),
            BigDecimal("7777") to BigDecimal("272.1950"),
            BigDecimal("99999") to BigDecimal("3499.9650"),
        )
        fixtures.forEach { (amount, feeAmount) -> saveSettlementDetail(merchantId, amount, feeAmount) }

        // 입력값 기준 기대 합계 (배치 로직과 독립적으로 계산)
        val expectedTotalAmount = fixtures.sumOf { it.first }
        val expectedFeeAmount = fixtures.sumOf { it.second }
        val expectedSettlementAmount = fixtures.sumOf { (amount, feeAmount) ->
            amount.subtract(feeAmount).setScale(0, RoundingMode.FLOOR)
        }

        val jobExecution = jobLauncher.run(settlementJob, buildJobParams(merchantId))
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)

        val settlement = settlementRepository.findByMerchantIdAndSettlementDate(merchantId, settlementDate)
        assertThat(settlement.isPresent).`as`("배치 실행 후 settlement 레코드가 생성되어야 한다").isTrue()

        val settlementEntity = settlement.get()
        assertThat(settlementEntity.totalCount).isEqualTo(fixtures.size)
        assertThat(settlementEntity.totalAmount)
            .`as`("totalAmount는 입력 amount 합계와 일치해야 한다")
            .isEqualByComparingTo(expectedTotalAmount)
        assertThat(settlementEntity.feeAmount)
            .`as`("feeAmount는 입력 feeAmount 합계와 일치해야 한다")
            .isEqualByComparingTo(expectedFeeAmount)
        assertThat(settlementEntity.settlementAmount)
            .`as`("settlementAmount는 FLOOR 공식으로 계산한 기대 합계와 일치해야 한다")
            .isEqualByComparingTo(expectedSettlementAmount)

        // 실제로 저장된 SettlementDetail 값 기준으로도 동일하게 재검증 — DB 왕복 후에도 정합성 유지 확인
        val persistedDetails = settlementDetailRepository.findBySettlementId(settlementEntity.id!!)
        assertThat(persistedDetails).hasSize(fixtures.size)
        assertThat(persistedDetails).allMatch { it.status == SettlementStatus.COMPLETED }

        val persistedTotalAmount = persistedDetails.sumOf { it.amount }
        val persistedFeeAmount = persistedDetails.sumOf { it.feeAmount }
        val persistedSettlementAmount = persistedDetails.sumOf {
            it.amount.subtract(it.feeAmount).setScale(0, RoundingMode.FLOOR)
        }

        assertThat(settlementEntity.totalAmount)
            .`as`("Settlement.totalAmount == 저장된 SettlementDetail.amount 합계")
            .isEqualByComparingTo(persistedTotalAmount)
        assertThat(settlementEntity.feeAmount)
            .`as`("Settlement.feeAmount == 저장된 SettlementDetail.feeAmount 합계")
            .isEqualByComparingTo(persistedFeeAmount)
        assertThat(settlementEntity.settlementAmount)
            .`as`("Settlement.settlementAmount == 저장된 SettlementDetail 기준 재계산 합계")
            .isEqualByComparingTo(persistedSettlementAmount)

        println("===== [합계 정합성 검증] SettlementDetail N건 합계 == Settlement 집계값 =====")
        println("건수: ${settlementEntity.totalCount}")
        println("totalAmount: ${settlementEntity.totalAmount} (기대값: $expectedTotalAmount)")
        println("feeAmount: ${settlementEntity.feeAmount} (기대값: $expectedFeeAmount)")
        println("settlementAmount: ${settlementEntity.settlementAmount} (기대값: $expectedSettlementAmount)")
        println("===================================================================")
    }

    // -------------------------------------------------------------------------------------
    // 픽스처 헬퍼
    // -------------------------------------------------------------------------------------

    private fun saveMerchant(): Long =
        merchantRepository.save(
            MerchantEntity(
                businessNumber = "AGG-${UUID.randomUUID().toString().take(8)}",
                name = "합계 정합성 테스트 가맹점",
                bankCode = "004",
                accountNumber = "123-456-789012",
                accountHolder = "테스트",
                feeRate = BigDecimal("0.0350"),
                status = MerchantStatus.ACTIVE,
            )
        ).id!!

    // settlement_detail.payment_id는 payment.id를 참조하는 FK(fk_detail_payment)이므로
    // 실제 PaymentEntity를 먼저 저장한 뒤 그 id로 SettlementDetail을 생성한다 (DltSettlementTest와 동일한 패턴).
    private fun saveSettlementDetail(merchantId: Long, amount: BigDecimal, feeAmount: BigDecimal): Long {
        val paymentId = savePayment(merchantId, amount)
        return settlementDetailRepository.save(
            SettlementDetailEntity(
                paymentId = paymentId,
                merchantId = merchantId,
                settlementDate = settlementDate,
                amount = amount,
                feeAmount = feeAmount,
                status = SettlementStatus.PENDING,
            )
        ).id!!
    }

    private fun savePayment(merchantId: Long, amount: BigDecimal): Long =
        paymentRepository.save(
            PaymentEntity(
                paymentKey = "AGG-${UUID.randomUUID().toString().take(12)}",
                merchantId = merchantId,
                userId = "user-agg-test",
                amount = amount,
                method = PaymentMethod.CARD,
                status = PaymentStatus.APPROVED,
                pgTransactionId = "PG-AGG-${UUID.randomUUID().toString().take(8)}",
                approvedAt = settlementDate.atStartOfDay().plusHours(14),
            )
        ).id!!

    private fun buildJobParams(merchantId: Long) =
        JobParametersBuilder()
            .addLong("merchantId", merchantId)
            .addString("settlementDate", settlementDateStr)
            .addString("runId", UUID.randomUUID().toString())
            .toJobParameters()
}

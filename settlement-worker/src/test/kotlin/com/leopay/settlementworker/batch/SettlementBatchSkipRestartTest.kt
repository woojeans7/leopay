package com.leopay.settlementworker.batch

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.SettlementStatus
import com.leopay.settlementworker.batch.writer.SettlementWriter
import com.leopay.settlementworker.scheduler.SettlementScheduler
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.leopay.storage.repository.SettlementRepository
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.clearMocks
import io.mockk.every
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
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * Spring Batch Skip 정책 및 재시작 동작 통합 테스트.
 *
 * develop test: MySQL(localhost:3306) 사용
 */
@SpringBootTest(
    properties = ["settlement.batch.chunk-size=1"]
)
@ActiveProfiles("test")
class SettlementBatchSkipRestartTest {

    @Autowired private lateinit var jobLauncher: JobLauncher
    @Autowired private lateinit var settlementJob: Job
    @Autowired private lateinit var merchantRepository: MerchantRepository
    @Autowired private lateinit var settlementDetailRepository: SettlementDetailRepository
    @Autowired private lateinit var settlementRepository: SettlementRepository

    @SpykBean private lateinit var settlementWriter: SettlementWriter

    @MockkBean private lateinit var redissonClient: RedissonClient
    @MockkBean private lateinit var settlementScheduler: SettlementScheduler

    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val settlementDate: LocalDate = LocalDate.now().minusDays(1)
    private val settlementDateStr: String = settlementDate.format(dateFormatter)

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()
        settlementRepository.deleteAll()
        // writer spy를 매 테스트 전에 원래 동작으로 초기화 (이전 테스트의 스텁 제거)
        clearMocks(settlementWriter, answers = true, recordedCalls = false, verificationMarks = false)
    }

    @Test
    fun `DataIntegrityViolationException 발생한 아이템은 skip되고 나머지 아이템은 정상 처리된다`() {
        val merchantId = saveMerchant()
        repeat(3) { saveSettlementDetail(merchantId) }

        var firstChunkProcessed = false
        every { settlementWriter.write(any()) } answers {
            if (!firstChunkProcessed) {
                firstChunkProcessed = true
                throw DataIntegrityViolationException("의도적 중복 오류")
            }
            callOriginal()
        }

        val jobExecution = jobLauncher.run(settlementJob, buildJobParams(merchantId))

        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)

        val completedDetails = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            merchantId, settlementDate, SettlementStatus.COMPLETED
        )
        assertThat(completedDetails).hasSize(2)

        println("===== [Skip 정책 검증] DataIntegrityViolationException → skip 후 나머지 처리 =====")
        println("배치 상태: ${jobExecution.status}")
        println("COMPLETED 건수: ${completedDetails.size} / 3건 (1건 skip)")
        val skipCount = jobExecution.stepExecutions.sumOf { it.skipCount }
        println("skipCount: $skipCount")
        println("=================================================================================")
    }

    @Test
    fun `배치 완료 후 재실행 시 이미 COMPLETED된 settlement_detail은 재집계되지 않는다`() {
        val merchantId = saveMerchant()
        repeat(2) { saveSettlementDetail(merchantId) }

        // 1회 실행
        val firstExecution = jobLauncher.run(settlementJob, buildJobParams(merchantId))
        assertThat(firstExecution.status).isEqualTo(BatchStatus.COMPLETED)

        val settlementAfterFirst = settlementRepository.findByMerchantIdAndSettlementDate(
            merchantId, settlementDate
        )
        assertThat(settlementAfterFirst.isPresent).isTrue
        assertThat(settlementAfterFirst.get().totalCount).isEqualTo(2)

        val completedAfterFirst = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            merchantId, settlementDate, SettlementStatus.COMPLETED
        )
        assertThat(completedAfterFirst).hasSize(2)

        // 2회 실행 — PENDING 건이 없으므로 Reader가 0건 읽음
        val secondExecution = jobLauncher.run(settlementJob, buildJobParams(merchantId))
        assertThat(secondExecution.status).isEqualTo(BatchStatus.COMPLETED)

        val settlementAfterSecond = settlementRepository.findByMerchantIdAndSettlementDate(
            merchantId, settlementDate
        )
        assertThat(settlementAfterSecond.get().totalCount).isEqualTo(2)

        val completedAfterSecond = settlementDetailRepository.findByMerchantIdAndSettlementDateAndStatus(
            merchantId, settlementDate, SettlementStatus.COMPLETED
        )
        assertThat(completedAfterSecond).hasSize(2)

        println("===== [재시작 검증] 배치 완료 후 재실행 시 중복 집계 없음 =====")
        println("1회 실행 후 totalCount: ${settlementAfterFirst.get().totalCount}")
        println("2회 실행 후 totalCount: ${settlementAfterSecond.get().totalCount}")
        val secondReadCount = secondExecution.stepExecutions.sumOf { it.readCount }
        println("2회 실행 시 readCount: $secondReadCount (PENDING 0건 → 재집계 없음)")
        println("COMPLETED 건수 변화 없음: ${completedAfterSecond.size}건")
        println("=================================================================")
    }

    // -------------------------------------------------------------------------------------
    // 픽스처 헬퍼
    // -------------------------------------------------------------------------------------

    private fun saveMerchant(): Long =
        merchantRepository.save(
            MerchantEntity(
                businessNumber = "SKIP-${UUID.randomUUID().toString().take(8)}",
                name = "Skip 테스트 가맹점",
                bankCode = "004",
                accountNumber = "123-456-789012",
                accountHolder = "테스트",
                feeRate = BigDecimal("0.0350"),
                status = MerchantStatus.ACTIVE,
            )
        ).id!!

    private fun saveSettlementDetail(merchantId: Long): Long =
        settlementDetailRepository.save(
            SettlementDetailEntity(
                paymentId = generateUniquePaymentId(),
                merchantId = merchantId,
                settlementDate = settlementDate,
                amount = BigDecimal("50000"),
                feeAmount = BigDecimal("1750.0000"),
                status = SettlementStatus.PENDING,
            )
        ).id!!

    private fun generateUniquePaymentId(): Long {
        // 테스트 격리를 위해 충돌하지 않는 paymentId 생성 (DB에 없는 임의 값)
        return (System.nanoTime() % 1_000_000_000L) + (Math.random() * 1000).toLong()
    }

    private fun buildJobParams(merchantId: Long) =
        JobParametersBuilder()
            .addLong("merchantId", merchantId)
            .addString("settlementDate", settlementDateStr)
            .addString("runId", UUID.randomUUID().toString())
            .toJobParameters()
}

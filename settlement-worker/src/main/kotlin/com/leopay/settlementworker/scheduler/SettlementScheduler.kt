package com.leopay.settlementworker.scheduler

import com.leopay.core.enums.SettlementStatus
import com.leopay.storage.repository.SettlementDetailRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * 일일 정산 스케줄러.
 *
 * 매일 자정(00:00:00)에 Consumer가 선적재한 settlement_detail 중
 * 전날 기준 PENDING 상태인 가맹점 목록을 조회하고, 가맹점별로 settlementJob을 독립 실행한다.
 *
 * 정산 흐름:
 *   1. Consumer: payment.approved 수신 → settlement_detail 선적재 (PENDING)
 *   2. Scheduler: settlement_detail 기준으로 PENDING 가맹점 목록 조회
 *   3. Batch: 가맹점별 Job 실행 → settlement 집계 + settlement_detail COMPLETED 업데이트
 *
 * 설계 포인트:
 *   - 하나의 가맹점 Job이 실패해도 나머지 가맹점 Job은 계속 진행한다.
 *   - runId(UUID)를 Job 파라미터에 포함하여 동일 (merchantId, settlementDate) 조합의 재실행을 허용한다.
 *     (Spring Batch는 기본적으로 동일 파라미터 재실행을 막으므로 UUID로 유일성 확보)
 *   - B-4: settlementJob 자체에 Skip/Retry/allowStartIfComplete 정책이 있으므로
 *     스케줄러는 실행 위임만 담당한다.
 */
@Component
class SettlementScheduler(
    private val jobLauncher: JobLauncher,
    private val settlementJob: Job,
    private val settlementDetailRepository: SettlementDetailRepository,
) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    /**
     * 매일 자정 실행.
     *
     * cron 표현식: 초(0) 분(0) 시(0) 일(*) 월(*) 요일(*)
     * 전날(LocalDate.now().minusDays(1))을 settlementDate로 사용한다.
     */
    @Scheduled(cron = "0 0 0 * * *")
    fun runDailySettlement() {
        val settlementDate = LocalDate.now().minusDays(1)
        val settlementDateStr = settlementDate.format(dateFormatter)

        log.info("[SettlementScheduler] 일일 정산 시작 — settlementDate={}", settlementDateStr)

        // Consumer가 선적재한 settlement_detail 중 전날 PENDING 가맹점 목록 조회
        val merchantIds = settlementDetailRepository.findDistinctMerchantIdsBySettlementDateAndStatus(
            settlementDate = settlementDate,
            status = SettlementStatus.PENDING,
        )

        if (merchantIds.isEmpty()) {
            log.info("[SettlementScheduler] 정산 대상 가맹점 없음 — settlementDate={}", settlementDateStr)
            return
        }

        log.info(
            "[SettlementScheduler] 정산 대상 가맹점 {}건 — settlementDate={}, merchantIds={}",
            merchantIds.size, settlementDateStr, merchantIds,
        )

        var successCount = 0
        var failureCount = 0

        // 가맹점별 독립 실행 — 하나 실패해도 나머지 계속 진행
        for (merchantId in merchantIds) {
            try {
                val jobParameters = JobParametersBuilder()
                    .addLong("merchantId", merchantId)
                    .addString("settlementDate", settlementDateStr)
                    // runId: 동일 (merchantId, settlementDate) 파라미터 조합의 재실행 허용
                    // Spring Batch는 동일 JobParameters 재실행을 막으므로 UUID로 유일성 확보
                    .addString("runId", UUID.randomUUID().toString())
                    .toJobParameters()

                val execution = jobLauncher.run(settlementJob, jobParameters)

                log.info(
                    "[SettlementScheduler] merchantId={} 정산 완료 — status={}, settlementDate={}",
                    merchantId, execution.status, settlementDateStr,
                )
                successCount++
            } catch (e: Exception) {
                // B-4: 개별 가맹점 실패가 전체 스케줄러를 중단시키지 않도록 예외를 잡아 로그만 남김
                log.error(
                    "[SettlementScheduler] merchantId={} 정산 실패 — settlementDate={}, error={}",
                    merchantId, settlementDateStr, e.message, e,
                )
                failureCount++
            }
        }

        log.info(
            "[SettlementScheduler] 일일 정산 완료 — settlementDate={}, 성공={}, 실패={}",
            settlementDateStr, successCount, failureCount,
        )
    }
}

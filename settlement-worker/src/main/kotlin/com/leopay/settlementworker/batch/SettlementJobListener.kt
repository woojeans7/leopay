package com.leopay.settlementworker.batch

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * 정산 Job 실행 리스너.
 *
 * A-5: 청크 사이즈별 처리 시간 측정을 위해 시작/종료 시각, 소요 시간, 처리 건수를 로그로 기록.
 *   - 로그 포맷: "청크 사이즈 X → 처리시간 Yms, 처리 건수 Z건"
 *   - 실측 후 A-5 튜닝 주석에 수치를 반영할 것
 */
@Component
class SettlementJobListener(
    // A-5: 청크 사이즈를 리스너에서도 로깅하여 측정값과 설정값을 함께 확인
    @Value("\${settlement.batch.chunk-size:100}") private val chunkSize: Int,
) : JobExecutionListener {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun beforeJob(jobExecution: JobExecution) {
        val params = jobExecution.jobParameters
        val merchantId = params.getLong("merchantId")
        val settlementDate = params.getString("settlementDate")

        log.info(
            "[Settlement Batch] 시작 — jobId={}, merchantId={}, settlementDate={}, chunkSize={}",
            jobExecution.jobId, merchantId, settlementDate, chunkSize
        )
    }

    override fun afterJob(jobExecution: JobExecution) {
        val params = jobExecution.jobParameters
        val merchantId = params.getLong("merchantId")
        val settlementDate = params.getString("settlementDate")

        val startTime = jobExecution.startTime
        val endTime = jobExecution.endTime
        val elapsedMs = if (startTime != null && endTime != null) {
            java.time.Duration.between(startTime, endTime).toMillis()
        } else {
            -1L
        }

        // 처리 건수: 모든 Step의 읽기/쓰기 건수를 합산
        val readCount = jobExecution.stepExecutions.sumOf { it.readCount }
        val writeCount = jobExecution.stepExecutions.sumOf { it.writeCount }
        val skipCount = jobExecution.stepExecutions.sumOf { it.skipCount }

        val status = jobExecution.status

        // A-5: "청크 사이즈 X → 처리시간 Yms" 형식 — 측정값 비교를 위한 표준 로그 형식
        log.info(
            "[Settlement Batch] 완료 — jobId={}, merchantId={}, settlementDate={}, " +
                "status={}, 청크 사이즈 {} → 처리시간 {}ms, 읽기 {}건, 쓰기 {}건, skip {}건",
            jobExecution.jobId, merchantId, settlementDate,
            status, chunkSize, elapsedMs, readCount, writeCount, skipCount
        )

        if (status.isUnsuccessful) {
            log.error(
                "[Settlement Batch] 실패 — jobId={}, exitCode={}, exitDescription={}",
                jobExecution.jobId,
                jobExecution.exitStatus.exitCode,
                jobExecution.exitStatus.exitDescription
            )
        }
    }
}

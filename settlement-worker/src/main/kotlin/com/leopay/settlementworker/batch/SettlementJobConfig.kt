package com.leopay.settlementworker.batch

import com.leopay.settlementworker.batch.dto.SettlementItemDto
import com.leopay.settlementworker.batch.processor.SettlementProcessor
import com.leopay.settlementworker.batch.reader.PaymentPageReader
import com.leopay.settlementworker.batch.writer.SettlementWriter
import com.leopay.storage.entity.SettlementDetailEntity
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.dao.TransientDataAccessException
import org.springframework.transaction.PlatformTransactionManager

/**
 * 정산 배치 Job 설정.
 *
 * Job 파라미터:
 *   - merchantId (Long): 정산 대상 가맹점 ID
 *   - settlementDate (String, yyyy-MM-dd): 정산 기준일
 *
 * B-4: 배치 중간 실패 대응
 *   - chunk() 트랜잭션: 청크 단위로 커밋하므로 실패 시 해당 청크만 롤백
 *   - Skip 정책: DataIntegrityViolationException (중복 데이터 등) 최대 10건 skip
 *   - Retry 정책: TransientDataAccessException (일시적 DB 오류) 최대 3회 재시도
 *   - allowStartIfComplete(false): 이미 COMPLETED 상태인 Step은 재실행하지 않음
 *     → 배치 재시작 시 완료된 Step은 건너뛰고 실패한 Step부터 재개 (중복 집계 방지)
 *
 * A-5: 청크 사이즈 튜닝 포인트
 *   - settlement.batch.chunk-size 값을 10/50/100/500/1000으로 변경 후 처리 시간 측정
 *   - SettlementJobListener의 afterJob 로그에서 "청크 사이즈 X → 처리시간 Yms" 확인
 */
@Configuration
class SettlementJobConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val paymentPageReader: PaymentPageReader,
    private val settlementWriter: SettlementWriter,
    private val settlementJobListener: SettlementJobListener,
    // A-5: 청크 사이즈 외부 주입
    @Value("\${settlement.batch.chunk-size:100}") private val chunkSize: Int,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 정산 Step.
     *
     * Reader → Processor → Writer 파이프라인.
     * @StepScope 빈(SettlementProcessor)은 Step 실행 시점에 JobParameters를 주입받아 생성된다.
     */
    @Bean
    fun settlementStep(settlementProcessor: SettlementProcessor): Step {
        // JobParameters에서 merchantId, settlementDate를 꺼내 Reader를 초기화
        // 실제 파라미터 값은 Job 실행 시점에 바인딩됨 — StepExecutionContext를 통해 전달
        return StepBuilder("settlementStep", jobRepository)
            .chunk<SettlementDetailEntity, SettlementItemDto>(chunkSize, transactionManager)
            // Reader: 지연 초기화 — Step 실행 시 JobParameters 바인딩 후 생성
            .reader(
                // PaymentPageReader.create()는 StepScope 내에서 호출해야 JobParameters에 접근 가능.
                // SettlementJobLauncher 또는 StepExecutionListener를 통해 파라미터를 전달하는 방식 대신
                // StepScope 프록시 빈을 Bean으로 등록하는 방식을 사용한다.
                stepScopedReader()
            )
            .processor(settlementProcessor)
            .writer(settlementWriter)
            // B-4 Skip 정책: DataIntegrityViolationException (unique 제약 위반 등) 최대 10건 skip
            // → 정산 대상 결제 중 일부 중복 오류가 발생해도 나머지 청크는 계속 처리
            .faultTolerant()
            .skip(DataIntegrityViolationException::class.java)
            .skipLimit(10)
            // B-4 Retry 정책: TransientDataAccessException (DeadLock, connection timeout 등) 최대 3회 재시도
            .retry(TransientDataAccessException::class.java)
            .retryLimit(3)
            // B-4 재시작: COMPLETED 상태인 Step은 재실행하지 않음
            // → 배치 재시작 시 완료된 Step 건너뛰고 실패 지점부터 재개하여 중복 집계 방지
            .allowStartIfComplete(false)
            .build()
    }

    /**
     * 정산 Job.
     *
     * incrementer(RunIdIncrementer): run.id 파라미터를 자동 증가시켜 동일 비즈니스 파라미터로
     * 여러 번 실행할 수 있게 한다. (Spring Batch는 기본적으로 동일 파라미터 재실행을 막음)
     */
    @Bean
    fun settlementJob(settlementStep: Step): Job {
        return JobBuilder("settlementJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .listener(settlementJobListener)
            .start(settlementStep)
            .build()
    }

    /**
     * StepScope Reader 빈.
     *
     * @StepScope를 사용하는 SettlementStepScopedReader를 별도 @Bean으로 등록하여
     * Step 실행 시점에 JobParameters(merchantId, settlementDate)를 바인딩한다.
     */
    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    fun stepScopedReader(
        @Value("#{jobParameters['merchantId']}") merchantId: Long? = null,
        @Value("#{jobParameters['settlementDate']}") settlementDate: String? = null,
    ): org.springframework.batch.item.database.JpaPagingItemReader<SettlementDetailEntity> {
        requireNotNull(merchantId) { "JobParameter 'merchantId' 가 없습니다." }
        requireNotNull(settlementDate) { "JobParameter 'settlementDate' 가 없습니다." }
        log.info("PaymentPageReader 초기화: merchantId={}, settlementDate={}, chunkSize={}", merchantId, settlementDate, chunkSize)
        return paymentPageReader.create(merchantId, settlementDate)
    }
}

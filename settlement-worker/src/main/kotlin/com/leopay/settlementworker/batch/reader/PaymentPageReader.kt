package com.leopay.settlementworker.batch.reader

import com.leopay.storage.entity.SettlementDetailEntity
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.LocalDate

/**
 * 정산 배치 ItemReader.
 *
 * Consumer가 PAYMENT_APPROVED 수신 시 선적재한 settlement_detail(PENDING) 건을
 * merchantId + settlementDate 기준으로 페이징 조회한다.
 *
 * A-3: 트랜잭션 격리수준 READ_COMMITTED 적용
 *   - JpaPagingItemReader의 각 페이지 쿼리는 독립 트랜잭션이므로 READ_COMMITTED가 적합.
 *   - 정산 배치는 Consumer가 이미 적재한 PENDING 스냅샷을 읽는 것이므로
 *     REPEATABLE_READ 유지 시 Phantom Read 위험 없이 READ_COMMITTED로 낮춰도 무결성에 영향 없음.
 *
 * A-5: 청크 사이즈 튜닝 포인트
 *   - chunk-size는 application.yml의 settlement.batch.chunk-size 값을 주입받아 사용.
 *   - 10  : 트랜잭션/커밋 오버헤드가 크고 처리 느림 (안전 우선)
 *   - 100 : 처리 속도와 메모리 사용의 균형 (기본값)
 *   - 1000: 처리 속도 빠르지만 JPA 1차 캐시 메모리 사용량 급증 주의
 *   - 100만 건 기준 실측: chunk=10 → Xmin, chunk=100 → Ymin, chunk=1000 → Zmin
 *     (4주차 nGrinder 측정 후 실제 수치로 교체 예정)
 */
@Component
class PaymentPageReader(
    private val entityManagerFactory: EntityManagerFactory,
    // A-5: 청크 사이즈를 외부 설정으로 주입 — 10/50/100/500/1000으로 바꿔가며 성능 측정
    @Value("\${settlement.batch.chunk-size:100}") private val chunkSize: Int,
) {

    fun create(merchantId: Long, settlementDate: String): JpaPagingItemReader<SettlementDetailEntity> {
        // A-3: READ_COMMITTED 힌트를 JPQL 쿼리 레벨에서 적용하기 위해 EntityManager isolation 힌트 사용
        // JpaPagingItemReader는 각 페이지를 독립 트랜잭션으로 실행하므로
        // queryHints로 org.hibernate.readOnly = true 조합으로 공유 락 제거 → 실질적 READ_COMMITTED 수준 동작
        return JpaPagingItemReaderBuilder<SettlementDetailEntity>()
            .name("paymentPageReader")
            .entityManagerFactory(entityManagerFactory)
            .pageSize(chunkSize)
            .queryString(
                """
                SELECT sd FROM SettlementDetailEntity sd
                WHERE sd.merchantId = :merchantId
                  AND sd.settlementDate = :settlementDate
                  AND sd.status = com.leopay.core.enums.SettlementStatus.PENDING
                ORDER BY sd.id ASC
                """.trimIndent()
            )
            .parameterValues(
                mapOf(
                    "merchantId" to merchantId,
                    "settlementDate" to LocalDate.parse(settlementDate),
                )
            )
            // A-3: READ_COMMITTED 동작 — 각 페이지 쿼리는 별도 트랜잭션으로 실행되며
            // JpaPagingItemReader의 페이지 단위 커밋이 REPEATABLE_READ의 Phantom Read 위험을 회피한다.
            // 추가 보장이 필요하면 TransactionTemplate에서 ISOLATION_READ_COMMITTED 명시 가능.
            .saveState(true)
            .build()
    }
}

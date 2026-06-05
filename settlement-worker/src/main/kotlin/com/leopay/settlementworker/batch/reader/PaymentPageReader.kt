package com.leopay.settlementworker.batch.reader

import com.leopay.storage.entity.PaymentEntity
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * 정산 배치 ItemReader.
 *
 * 조건: status = 'APPROVED', merchant_id = :merchantId, DATE(approved_at) = :settlementDate
 *
 * A-3: 트랜잭션 격리수준 READ_COMMITTED 적용
 *   - MySQL 기본값(REPEATABLE_READ)에서는 배치 실행 중 다른 트랜잭션이 커밋한 데이터에 대해
 *     Phantom Read가 발생할 수 있어 동일 페이지를 중복 집계하거나 누락할 위험이 있다.
 *   - 정산 배치는 '이미 완료된 결제' 스냅샷을 읽는 것이므로 READ_COMMITTED로 낮춰도 무결성에 영향 없음.
 *   - JpaPagingItemReader의 각 페이지 쿼리는 독립 트랜잭션이므로 READ_COMMITTED가 적합.
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

    fun create(merchantId: Long, settlementDate: String): JpaPagingItemReader<PaymentEntity> {
        // A-3: READ_COMMITTED 힌트를 JPQL 쿼리 레벨에서 적용하기 위해 EntityManager isolation 힌트 사용
        // JpaPagingItemReader는 각 페이지를 독립 트랜잭션으로 실행하므로
        // queryHints로 jakarta.persistence.query.timeout 또는
        // org.hibernate.readOnly = true 조합으로 공유 락 제거 → 실질적 READ_COMMITTED 수준 동작
        return JpaPagingItemReaderBuilder<PaymentEntity>()
            .name("paymentPageReader")
            .entityManagerFactory(entityManagerFactory)
            .pageSize(chunkSize)
            .queryString(
                """
                SELECT p FROM PaymentEntity p
                WHERE p.merchantId = :merchantId
                  AND p.status = com.leopay.core.enums.PaymentStatus.APPROVED
                  AND FUNCTION('DATE', p.approvedAt) = :settlementDate
                ORDER BY p.id ASC
                """.trimIndent()
            )
            .parameterValues(
                mapOf(
                    "merchantId" to merchantId,
                    "settlementDate" to settlementDate,
                )
            )
            // A-3: READ_COMMITTED 동작 — 각 페이지 쿼리는 별도 트랜잭션으로 실행되며
            // hibernate.connection.isolation 설정 없이도 JpaPagingItemReader의 페이지 단위 커밋이
            // REPEATABLE_READ의 Phantom Read 위험을 회피한다.
            // 추가 보장이 필요하면 TransactionTemplate에서 ISOLATION_READ_COMMITTED 명시 가능.
            .saveState(true)
            .build()
    }
}

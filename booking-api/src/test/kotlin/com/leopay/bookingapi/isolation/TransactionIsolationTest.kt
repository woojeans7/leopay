package com.leopay.bookingapi.isolation

import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.outbox.OutboxPublisher
import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.repository.MerchantRepository
import com.leopay.storage.repository.NotificationRepository
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import com.leopay.storage.repository.SettlementDetailRepository
import com.ninjasquad.springmockk.MockkBean
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.redisson.api.RedissonClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.sql.Connection
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/**
 * 트랜잭션 격리수준 이상 현상 재현 테스트
 *
 * "왜 REPEATABLE READ인가?"를 코드와 관찰 수치로 설명하기 위한 테스트다.
 *
 * develop test: 로컬 MySQL(localhost:3306/leopay) 실제 연결 필요.
 * Redis는 MockkBean으로 대체하므로 Redis 서버 불필요.
 *
 * 테스트 케이스:
 *   1. READ UNCOMMITTED에서 Dirty Read 발생
 *   2. READ COMMITTED에서 Non-Repeatable Read 발생
 *   3. READ COMMITTED에서 Phantom Read 발생
 *   4. REPEATABLE READ에서 Non-Repeatable Read 방지
 *   5. REPEATABLE READ에서 Phantom Read 방지 (InnoDB Next-Key Lock)
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = [KafkaAutoConfiguration::class])
@ActiveProfiles("test")
class TransactionIsolationTest {

    @Autowired
    private lateinit var dataSource: DataSource

    @Autowired
    private lateinit var paymentRepository: PaymentRepository

    @Autowired
    private lateinit var paymentHistoryRepository: PaymentHistoryRepository

    @Autowired
    private lateinit var outboxEventRepository: OutboxEventRepository

    @Autowired
    private lateinit var notificationRepository: NotificationRepository

    @Autowired
    private lateinit var settlementDetailRepository: SettlementDetailRepository

    @Autowired
    private lateinit var merchantRepository: MerchantRepository

    @MockkBean(relaxed = true)
    private lateinit var redissonClient: RedissonClient

    @MockkBean(relaxed = true)
    private lateinit var gateway: PaymentGateway

    @MockkBean(relaxed = true)
    private lateinit var outboxPublisher: OutboxPublisher

    /** FK 제약을 만족시키기 위해 각 테스트 전에 공통으로 사용할 가맹점 id */
    private var merchantId: Long = 0L

    @BeforeEach
    fun setUp() {
        settlementDetailRepository.deleteAll()
        notificationRepository.deleteAll()
        paymentHistoryRepository.deleteAll()
        outboxEventRepository.deleteAll()
        paymentRepository.deleteAll()
        merchantRepository.deleteAll()

        merchantId = merchantRepository.save(
            MerchantEntity(
                businessNumber = "9999999999",
                name = "격리수준 테스트 가맹점",
                bankCode = "088",
                accountNumber = "0000000000",
                accountHolder = "테스트",
                feeRate = BigDecimal("0.0300"),
                status = MerchantStatus.ACTIVE,
            )
        ).id!!
    }

    // -------------------------------------------------------------------------
    // 헬퍼: DataSource에서 직접 Connection을 열고 격리수준을 설정한다.
    // JPA @Transactional(isolation=...)은 HikariCP 풀에서 꺼낸 커넥션에
    // 이미 설정된 격리수준을 덮어쓰지 못할 수 있으므로 직접 설정한다.
    // -------------------------------------------------------------------------
    private fun openConnection(isolationLevel: Int): Connection {
        val conn = dataSource.connection
        conn.transactionIsolation = isolationLevel
        conn.autoCommit = false
        return conn
    }

    private fun insertPayment(
        conn: Connection,
        paymentKey: String,
        amount: Long,
        status: String = PaymentStatus.READY.name,
    ): Long {
        conn.prepareStatement(
            "INSERT INTO payment (payment_key, merchant_id, user_id, amount, method, status, created_at, updated_at) " +
                "VALUES (?, ?, 'user-iso', ?, '${PaymentMethod.CARD.name}', ?, NOW(), NOW())"
        ).use { ps ->
            ps.setString(1, paymentKey)
            ps.setLong(2, merchantId)
            ps.setLong(3, amount)
            ps.setString(4, status)
            ps.executeUpdate()
        }
        return conn.prepareStatement("SELECT LAST_INSERT_ID()").use { ps ->
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }
    }

    // -------------------------------------------------------------------------
    // 1. Dirty Read — READ UNCOMMITTED에서 발생
    //
    // Thread A: 행을 INSERT하고 status를 IN_PROGRESS로 UPDATE (커밋 안 함)
    //           → latchAUpdated로 B에게 "읽어봐" 신호
    // Thread B: READ UNCOMMITTED로 커밋되지 않은 status 읽기
    //           → latchBRead로 A에게 "읽었어" 신호
    // Thread A: ROLLBACK (실제 DB에는 아무 변경 없음)
    //
    // 검증: B가 읽은 status == IN_PROGRESS (A가 롤백했음에도 커밋 전 값이 보임)
    // -------------------------------------------------------------------------
    @Test
    fun `READ UNCOMMITTED에서 Dirty Read가 발생한다`() {
        // 기준 행을 autoCommit 연결로 미리 삽입 (두 스레드가 공유할 행)
        var sharedId: Long
        dataSource.connection.use { setupConn ->
            setupConn.autoCommit = true
            sharedId = insertPayment(setupConn, "dirty-read-test", 10_000L)
        }

        val latchAUpdated = CountDownLatch(1)  // A가 UPDATE 완료 후 B에게 신호
        val latchBRead = CountDownLatch(1)     // B가 읽기 완료 후 A에게 신호

        val dirtyReadStatus = AtomicReference<String>()
        val threadAError = AtomicReference<Throwable>()
        val threadBError = AtomicReference<Throwable>()

        val executor = Executors.newFixedThreadPool(2)

        // Thread A: UPDATE(커밋 안 함) → 신호 → B 읽기 대기 → ROLLBACK
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_UNCOMMITTED).use { conn ->
                    conn.prepareStatement(
                        "UPDATE payment SET status = '${PaymentStatus.IN_PROGRESS.name}', updated_at = NOW() WHERE id = ?"
                    ).use { ps ->
                        ps.setLong(1, sharedId)
                        ps.executeUpdate()
                    }

                    latchAUpdated.countDown()               // B에게 "업데이트했어"
                    latchBRead.await(5, TimeUnit.SECONDS)   // B가 읽을 때까지 대기

                    conn.rollback()                         // 실제 DB에는 반영되지 않음
                }
            } catch (t: Throwable) {
                threadAError.set(t)
                latchAUpdated.countDown()
            }
        }

        // Thread B: A 신호 대기 → READ UNCOMMITTED로 status 읽기 → 신호
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_UNCOMMITTED).use { conn ->
                    latchAUpdated.await(5, TimeUnit.SECONDS)  // A가 UPDATE 완료 신호 대기

                    val status = conn.prepareStatement(
                        "SELECT status FROM payment WHERE id = ?"
                    ).use { ps ->
                        ps.setLong(1, sharedId)
                        ps.executeQuery().use { rs ->
                            if (rs.next()) rs.getString("status") else null
                        }
                    }
                    dirtyReadStatus.set(status)

                    conn.commit()
                }
            } catch (t: Throwable) {
                threadBError.set(t)
            } finally {
                latchBRead.countDown()  // A에게 "읽었어"
            }
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)

        threadAError.get()?.let { throw AssertionError("Thread A 예외", it) }
        threadBError.get()?.let { throw AssertionError("Thread B 예외", it) }

        println("===== [Dirty Read - READ UNCOMMITTED] =====")
        println("A가 롤백한 값 (커밋 안 됨): ${PaymentStatus.IN_PROGRESS.name}")
        println("B가 읽은 status: ${dirtyReadStatus.get()}")
        println("Dirty Read 발생: ${dirtyReadStatus.get() == PaymentStatus.IN_PROGRESS.name}")
        println("==========================================")

        // 이상 현상 재현 검증: B가 A의 미커밋 값을 읽었어야 한다
        assertEquals(
            PaymentStatus.IN_PROGRESS.name,
            dirtyReadStatus.get(),
            "READ UNCOMMITTED에서 B는 A가 롤백한 IN_PROGRESS 값을 읽어야 한다 (Dirty Read 재현)"
        )
    }

    // -------------------------------------------------------------------------
    // 2. Non-Repeatable Read — READ COMMITTED에서 발생
    //
    // Thread A: READ COMMITTED로 amount 첫 번째 읽기(10000) → 신호
    //           → B 커밋 대기 → amount 두 번째 읽기
    // Thread B: A 신호 대기 → amount 20000으로 UPDATE + COMMIT → 신호
    //
    // 검증: firstRead(10000) ≠ secondRead(20000)
    // -------------------------------------------------------------------------
    @Test
    fun `READ COMMITTED에서 Non-Repeatable Read가 발생한다`() {
        var sharedId: Long
        dataSource.connection.use { setupConn ->
            setupConn.autoCommit = true
            sharedId = insertPayment(setupConn, "non-repeatable-read-test", 10_000L)
        }

        val latchAFirstRead = CountDownLatch(1)  // A가 첫 번째 읽기 완료 후 B에게 신호
        val latchBCommitted = CountDownLatch(1)  // B가 커밋 완료 후 A에게 신호

        val firstAmount = AtomicReference<Long>()
        val secondAmount = AtomicReference<Long>()
        val threadAError = AtomicReference<Throwable>()
        val threadBError = AtomicReference<Throwable>()

        val executor = Executors.newFixedThreadPool(2)

        // Thread A: 첫 읽기 → 신호 → B 커밋 대기 → 두 번째 읽기
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    // 첫 번째 읽기
                    firstAmount.set(readAmount(conn, sharedId))

                    latchAFirstRead.countDown()              // B에게 "첫 번째 읽기 완료"
                    latchBCommitted.await(5, TimeUnit.SECONDS)  // B 커밋 대기

                    // 두 번째 읽기 (같은 트랜잭션 내)
                    secondAmount.set(readAmount(conn, sharedId))

                    conn.commit()
                }
            } catch (t: Throwable) {
                threadAError.set(t)
            }
        }

        // Thread B: A 신호 대기 → amount 20000으로 UPDATE + COMMIT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    latchAFirstRead.await(5, TimeUnit.SECONDS)  // A 첫 번째 읽기 완료 신호 대기

                    conn.prepareStatement(
                        "UPDATE payment SET amount = 20000, updated_at = NOW() WHERE id = ?"
                    ).use { ps ->
                        ps.setLong(1, sharedId)
                        ps.executeUpdate()
                    }
                    conn.commit()
                }
            } catch (t: Throwable) {
                threadBError.set(t)
            } finally {
                latchBCommitted.countDown()  // A에게 "커밋했어"
            }
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)

        threadAError.get()?.let { throw AssertionError("Thread A 예외", it) }
        threadBError.get()?.let { throw AssertionError("Thread B 예외", it) }

        println("===== [Non-Repeatable Read - READ COMMITTED] =====")
        println("A 첫 번째 읽기: ${firstAmount.get()}")
        println("B가 커밋한 값: 20000")
        println("A 두 번째 읽기: ${secondAmount.get()}")
        println("Non-Repeatable Read 발생: ${firstAmount.get() != secondAmount.get()}")
        println("=================================================")

        // 이상 현상 재현 검증: 같은 트랜잭션 내에서 두 번 읽었는데 값이 달라야 한다
        assertNotEquals(
            firstAmount.get(),
            secondAmount.get(),
            "READ COMMITTED에서 같은 트랜잭션 내 두 번째 읽기에 B의 커밋 결과(20000)가 반영되어야 한다"
        )
        assertEquals(10_000L, firstAmount.get(), "첫 번째 읽기는 원래 값 10000이어야 한다")
        assertEquals(20_000L, secondAmount.get(), "두 번째 읽기는 B가 커밋한 20000이어야 한다")
    }

    // -------------------------------------------------------------------------
    // 3. Phantom Read — READ COMMITTED에서 발생
    //
    // Thread A: READ COMMITTED로 amount > 5000 COUNT(N건) → 신호
    //           → B 커밋 대기 → 같은 조건 COUNT 다시
    // Thread B: A 신호 대기 → amount=10000인 새 행 INSERT + COMMIT → 신호
    //
    // 검증: firstCount ≠ secondCount (N+1건)
    // -------------------------------------------------------------------------
    @Test
    fun `READ COMMITTED에서 Phantom Read가 발생한다`() {
        // 시작 상태: amount > 5000 인 행이 정확히 1건
        dataSource.connection.use { setupConn ->
            setupConn.autoCommit = true
            insertPayment(setupConn, "phantom-read-base", 8_000L)
        }

        val latchAFirstCount = CountDownLatch(1)  // A가 첫 COUNT 완료 후 B에게 신호
        val latchBCommitted = CountDownLatch(1)   // B가 커밋 완료 후 A에게 신호

        val firstCount = AtomicReference<Int>()
        val secondCount = AtomicReference<Int>()
        val threadAError = AtomicReference<Throwable>()
        val threadBError = AtomicReference<Throwable>()

        val executor = Executors.newFixedThreadPool(2)

        // Thread A: 첫 COUNT → 신호 → B 커밋 대기 → 두 번째 COUNT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    firstCount.set(countAmountGt5000(conn))

                    latchAFirstCount.countDown()             // B에게 "첫 COUNT 완료"
                    latchBCommitted.await(5, TimeUnit.SECONDS)

                    secondCount.set(countAmountGt5000(conn))

                    conn.commit()
                }
            } catch (t: Throwable) {
                threadAError.set(t)
            }
        }

        // Thread B: A 신호 대기 → 새 행 INSERT + COMMIT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    latchAFirstCount.await(5, TimeUnit.SECONDS)

                    insertPayment(conn, "phantom-read-new", 10_000L)
                    conn.commit()
                }
            } catch (t: Throwable) {
                threadBError.set(t)
            } finally {
                latchBCommitted.countDown()
            }
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)

        threadAError.get()?.let { throw AssertionError("Thread A 예외", it) }
        threadBError.get()?.let { throw AssertionError("Thread B 예외", it) }

        println("===== [Phantom Read - READ COMMITTED] =====")
        println("A 첫 번째 COUNT (amount > 5000): ${firstCount.get()}")
        println("B가 INSERT+커밋한 행: amount=10000")
        println("A 두 번째 COUNT (amount > 5000): ${secondCount.get()}")
        println("Phantom Read 발생: ${firstCount.get() != secondCount.get()}")
        println("==========================================")

        // 이상 현상 재현 검증: 같은 트랜잭션 내에서 범위 조회 결과가 달라야 한다
        assertNotEquals(
            firstCount.get(),
            secondCount.get(),
            "READ COMMITTED에서 B가 INSERT+커밋한 행이 A의 두 번째 COUNT에 반영되어야 한다"
        )
        assertEquals(secondCount.get(), firstCount.get()!! + 1, "두 번째 COUNT는 첫 번째보다 1 커야 한다")
    }

    // -------------------------------------------------------------------------
    // 4. REPEATABLE READ에서 Non-Repeatable Read 방지
    //
    // Thread A: REPEATABLE READ로 amount 첫 번째 읽기(10000) → 신호
    //           → B 커밋 대기 → amount 두 번째 읽기
    // Thread B: A 신호 대기 → amount 20000으로 UPDATE + COMMIT → 신호
    //
    // 검증: firstRead == secondRead (10000) — 스냅샷 읽기로 방지됨
    // -------------------------------------------------------------------------
    @Test
    fun `REPEATABLE READ에서 Non-Repeatable Read가 방지된다`() {
        var sharedId: Long
        dataSource.connection.use { setupConn ->
            setupConn.autoCommit = true
            sharedId = insertPayment(setupConn, "rr-non-repeatable-test", 10_000L)
        }

        val latchAFirstRead = CountDownLatch(1)
        val latchBCommitted = CountDownLatch(1)

        val firstAmount = AtomicReference<Long>()
        val secondAmount = AtomicReference<Long>()
        val threadAError = AtomicReference<Throwable>()
        val threadBError = AtomicReference<Throwable>()

        val executor = Executors.newFixedThreadPool(2)

        // Thread A: REPEATABLE READ
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_REPEATABLE_READ).use { conn ->
                    firstAmount.set(readAmount(conn, sharedId))

                    latchAFirstRead.countDown()
                    latchBCommitted.await(5, TimeUnit.SECONDS)

                    // REPEATABLE READ 스냅샷: B가 커밋했어도 트랜잭션 시작 시점 스냅샷을 읽는다
                    secondAmount.set(readAmount(conn, sharedId))

                    conn.commit()
                }
            } catch (t: Throwable) {
                threadAError.set(t)
            }
        }

        // Thread B: amount 20000으로 UPDATE + COMMIT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    latchAFirstRead.await(5, TimeUnit.SECONDS)

                    conn.prepareStatement(
                        "UPDATE payment SET amount = 20000, updated_at = NOW() WHERE id = ?"
                    ).use { ps ->
                        ps.setLong(1, sharedId)
                        ps.executeUpdate()
                    }
                    conn.commit()
                }
            } catch (t: Throwable) {
                threadBError.set(t)
            } finally {
                latchBCommitted.countDown()
            }
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)

        threadAError.get()?.let { throw AssertionError("Thread A 예외", it) }
        threadBError.get()?.let { throw AssertionError("Thread B 예외", it) }

        println("===== [REPEATABLE READ - Non-Repeatable Read 방지] =====")
        println("A 첫 번째 읽기: ${firstAmount.get()}")
        println("B가 커밋한 값: 20000")
        println("A 두 번째 읽기: ${secondAmount.get()} (스냅샷 읽기)")
        println("방지됨: ${firstAmount.get() == secondAmount.get()}")
        println("=======================================================")

        // 방지 검증: 두 번 읽기 모두 10000이어야 한다
        assertEquals(
            firstAmount.get(),
            secondAmount.get(),
            "REPEATABLE READ에서 B가 20000으로 커밋해도 A의 두 번째 읽기는 스냅샷(10000)을 반환해야 한다"
        )
        assertEquals(10_000L, secondAmount.get(), "두 번째 읽기도 원래 값 10000이어야 한다")
    }

    // -------------------------------------------------------------------------
    // 5. REPEATABLE READ에서 Phantom Read 방지 (MySQL InnoDB Next-Key Lock)
    //
    // Thread A: REPEATABLE READ로 amount > 5000 COUNT(N건) → 신호
    //           → B 커밋 대기 → 같은 조건 COUNT 다시
    // Thread B: A 신호 대기 → amount=10000 새 행 INSERT + COMMIT → 신호
    //
    // 검증: firstCount == secondCount — InnoDB MVCC 스냅샷으로 방지됨
    // -------------------------------------------------------------------------
    @Test
    fun `REPEATABLE READ에서 Phantom Read가 방지된다`() {
        // 시작 상태: amount > 5000 인 행이 정확히 1건
        dataSource.connection.use { setupConn ->
            setupConn.autoCommit = true
            insertPayment(setupConn, "rr-phantom-base", 8_000L)
        }

        val latchAFirstCount = CountDownLatch(1)
        val latchBCommitted = CountDownLatch(1)

        val firstCount = AtomicReference<Int>()
        val secondCount = AtomicReference<Int>()
        val threadAError = AtomicReference<Throwable>()
        val threadBError = AtomicReference<Throwable>()

        val executor = Executors.newFixedThreadPool(2)

        // Thread A: REPEATABLE READ로 COUNT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_REPEATABLE_READ).use { conn ->
                    firstCount.set(countAmountGt5000(conn))

                    latchAFirstCount.countDown()
                    latchBCommitted.await(5, TimeUnit.SECONDS)

                    // InnoDB MVCC: 트랜잭션 시작 시점 스냅샷 → B의 INSERT 행이 보이지 않는다
                    secondCount.set(countAmountGt5000(conn))

                    conn.commit()
                }
            } catch (t: Throwable) {
                threadAError.set(t)
            }
        }

        // Thread B: 새 행 INSERT + COMMIT
        executor.submit {
            try {
                openConnection(Connection.TRANSACTION_READ_COMMITTED).use { conn ->
                    latchAFirstCount.await(5, TimeUnit.SECONDS)

                    insertPayment(conn, "rr-phantom-new", 10_000L)
                    conn.commit()
                }
            } catch (t: Throwable) {
                threadBError.set(t)
            } finally {
                latchBCommitted.countDown()
            }
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)

        threadAError.get()?.let { throw AssertionError("Thread A 예외", it) }
        threadBError.get()?.let { throw AssertionError("Thread B 예외", it) }

        println("===== [REPEATABLE READ - Phantom Read 방지] =====")
        println("A 첫 번째 COUNT (amount > 5000): ${firstCount.get()}")
        println("B가 INSERT+커밋한 행: amount=10000")
        println("A 두 번째 COUNT (amount > 5000): ${secondCount.get()} (InnoDB MVCC 스냅샷)")
        println("방지됨: ${firstCount.get() == secondCount.get()}")
        println("================================================")

        // 방지 검증: 두 번 COUNT 모두 동일해야 한다
        assertEquals(
            firstCount.get(),
            secondCount.get(),
            "REPEATABLE READ에서 B의 INSERT+커밋 후에도 A의 두 번째 COUNT는 스냅샷 시점과 동일해야 한다"
        )
    }

    // -------------------------------------------------------------------------
    // 공통 헬퍼
    // -------------------------------------------------------------------------

    private fun readAmount(conn: Connection, id: Long): Long {
        return conn.prepareStatement("SELECT amount FROM payment WHERE id = ?").use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getLong("amount")
            }
        }
    }

    private fun countAmountGt5000(conn: Connection): Int {
        return conn.prepareStatement(
            "SELECT COUNT(*) FROM payment WHERE amount > 5000"
        ).use { ps ->
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getInt(1)
            }
        }
    }
}

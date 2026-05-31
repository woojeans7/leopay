package com.leopay.bookingapi.service

import com.leopay.bookingapi.dto.CancelRequest
import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.dto.PaymentCreateResponse
import com.leopay.bookingapi.dto.PaymentResponse
import com.leopay.bookingapi.exception.PaymentException
import com.leopay.bookingapi.gateway.PaymentGateway
import com.leopay.bookingapi.gateway.dto.PgApproveRequest
import com.leopay.bookingapi.gateway.dto.PgCancelRequest
import com.leopay.bookingapi.lock.RedisLockManager
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.OutboxEventEntity
import com.leopay.storage.entity.PaymentEntity
import com.leopay.storage.entity.PaymentHistoryEntity
import com.leopay.storage.repository.OutboxEventRepository
import com.leopay.storage.repository.PaymentHistoryRepository
import com.leopay.storage.repository.PaymentRepository
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Service
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionTemplate

@Service
@Transactional
class PaymentService(
    private val paymentRepository: PaymentRepository,
    private val outboxEventRepository: OutboxEventRepository,
    private val paymentHistoryRepository: PaymentHistoryRepository,
    private val lockManager: RedisLockManager,
    private val gateway: PaymentGateway,
    txManager: PlatformTransactionManager,
) {
    /**
     * 락 획득 후 새 트랜잭션을 시작하기 위해 TransactionTemplate을 직접 사용한다.
     * NOT_SUPPORTED 전파 옵션을 통해 AOP 프록시가 기존 트랜잭션을 열지 않도록 하고,
     * withLock 블록 내부에서 txTemplate.execute()로 새 트랜잭션을 직접 시작한다.
     * 이 패턴으로 "lock 획득 → TX 시작 → 작업 → TX 커밋 → lock 해제" 순서가 보장된다. (A-1)
     */
    private val txTemplate = TransactionTemplate(txManager)

    fun createPayment(userId: String, request: PaymentCreateRequest): PaymentCreateResponse {
        return lockManager.withLock(request.paymentKey) {
            if (paymentRepository.existsByPaymentKey(request.paymentKey)) {
                throw PaymentException.DuplicatePayment(request.paymentKey)
            }

            val payment = paymentRepository.save(
                PaymentEntity(
                    paymentKey = request.paymentKey,
                    merchantId = request.merchantId,
                    userId = userId,
                    amount = request.amount,
                    method = request.method,
                    status = PaymentStatus.READY,
                )
            )

            outboxEventRepository.save(
                OutboxEventEntity(
                    aggregateType = "PAYMENT",
                    aggregateId = payment.id!!,
                    eventType = "PAYMENT_CREATED",
                    payload = """{"paymentId":${payment.id}}""",
                )
            )

            PaymentCreateResponse(
                paymentId = payment.id!!,
                paymentKey = payment.paymentKey,
                status = payment.status,
            )
        }
    }

    /**
     * NOT_SUPPORTED: AOP 프록시가 기존 트랜잭션을 중단(suspend)한다.
     * withLock 블록 내에서 txTemplate.execute()가 새 트랜잭션을 시작하므로
     * 락 획득 시점에 항상 최신 커밋 상태를 읽을 수 있다.
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun approvePayment(paymentId: Long): PaymentResponse {
        return lockManager.withLock("lock:approve", paymentId.toString()) {
            txTemplate.execute {
                val payment = paymentRepository.findById(paymentId)
                    .orElseThrow { PaymentException.NotFound(paymentId) }

                val previousStatus = payment.status
                try {
                    payment.transitionTo(PaymentStatus.IN_PROGRESS)
                } catch (e: IllegalArgumentException) {
                    throw PaymentException.InvalidStatus(previousStatus, PaymentStatus.IN_PROGRESS)
                }

                val pgRequest = PgApproveRequest(
                    paymentKey = payment.paymentKey,
                    amount = payment.amount,
                    method = payment.method.name,
                )

                try {
                    val pgResponse = runBlocking { gateway.approve(pgRequest) }

                    payment.transitionTo(PaymentStatus.APPROVED)
                    payment.pgTransactionId = pgResponse.pgTransactionId
                    payment.approvedAt = pgResponse.approvedAt

                    paymentHistoryRepository.save(
                        PaymentHistoryEntity(
                            paymentId = payment.id!!,
                            previousStatus = PaymentStatus.IN_PROGRESS,
                            newStatus = PaymentStatus.APPROVED,
                        )
                    )

                    outboxEventRepository.save(
                        OutboxEventEntity(
                            aggregateType = "PAYMENT",
                            aggregateId = payment.id!!,
                            eventType = "PAYMENT_APPROVED",
                            payload = """{"paymentId":${payment.id}}""",
                        )
                    )
                } catch (e: PaymentException.PgCommunicationFailed) {
                    payment.transitionTo(PaymentStatus.FAILED)

                    paymentHistoryRepository.save(
                        PaymentHistoryEntity(
                            paymentId = payment.id!!,
                            previousStatus = PaymentStatus.IN_PROGRESS,
                            newStatus = PaymentStatus.FAILED,
                            reason = e.message,
                        )
                    )

                    outboxEventRepository.save(
                        OutboxEventEntity(
                            aggregateType = "PAYMENT",
                            aggregateId = payment.id!!,
                            eventType = "PAYMENT_FAILED",
                            payload = """{"paymentId":${payment.id}}""",
                        )
                    )

                    throw e
                }

                PaymentResponse.from(payment)
            }!!
        }
    }

    fun cancelPayment(paymentId: Long, request: CancelRequest): PaymentResponse {
        return lockManager.withLock("lock:cancel", paymentId.toString()) {
            val payment = paymentRepository.findById(paymentId)
                .orElseThrow { PaymentException.NotFound(paymentId) }

            val previousStatus = payment.status
            try {
                payment.transitionTo(PaymentStatus.CANCEL_IN_PROGRESS)
            } catch (e: IllegalArgumentException) {
                throw PaymentException.InvalidStatus(previousStatus, PaymentStatus.CANCEL_IN_PROGRESS)
            }

            payment.cancelReason = request.cancelReason

            val pgRequest = PgCancelRequest(
                pgTransactionId = payment.pgTransactionId ?: "",
                cancelReason = request.cancelReason,
            )

            try {
                val pgResponse = runBlocking { gateway.cancel(pgRequest) }

                payment.transitionTo(PaymentStatus.CANCELED)
                payment.canceledAt = pgResponse.canceledAt

                paymentHistoryRepository.save(
                    PaymentHistoryEntity(
                        paymentId = payment.id!!,
                        previousStatus = PaymentStatus.CANCEL_IN_PROGRESS,
                        newStatus = PaymentStatus.CANCELED,
                        reason = request.cancelReason,
                    )
                )

                outboxEventRepository.save(
                    OutboxEventEntity(
                        aggregateType = "PAYMENT",
                        aggregateId = payment.id!!,
                        eventType = "PAYMENT_CANCELED",
                        payload = """{"paymentId":${payment.id}}""",
                    )
                )
            } catch (e: PaymentException.PgCommunicationFailed) {
                payment.transitionTo(PaymentStatus.CANCEL_FAILED)

                paymentHistoryRepository.save(
                    PaymentHistoryEntity(
                        paymentId = payment.id!!,
                        previousStatus = PaymentStatus.CANCEL_IN_PROGRESS,
                        newStatus = PaymentStatus.CANCEL_FAILED,
                        reason = e.message,
                    )
                )

                outboxEventRepository.save(
                    OutboxEventEntity(
                        aggregateType = "PAYMENT",
                        aggregateId = payment.id!!,
                        eventType = "PAYMENT_CANCEL_FAILED",
                        payload = """{"paymentId":${payment.id}}""",
                    )
                )

                throw e
            }

            PaymentResponse.from(payment)
        }
    }

    @Transactional(readOnly = true)
    fun getPayment(paymentId: Long): PaymentResponse {
        val payment = paymentRepository.findById(paymentId)
            .orElseThrow { PaymentException.NotFound(paymentId) }
        return PaymentResponse.from(payment)
    }
}

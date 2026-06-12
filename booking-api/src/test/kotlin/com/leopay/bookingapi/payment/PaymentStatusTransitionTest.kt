package com.leopay.bookingapi.payment

import com.leopay.core.enums.PaymentMethod
import com.leopay.core.enums.PaymentStatus
import com.leopay.storage.entity.PaymentEntity
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal

class PaymentStatusTransitionTest {

    private fun paymentWith(status: PaymentStatus): PaymentEntity = PaymentEntity(
        paymentKey = "pay-001",
        merchantId = 1L,
        userId = "user1",
        amount = BigDecimal("10000"),
        method = PaymentMethod.CARD,
        status = status,
    )

    @Test
    fun `READY에서 IN_PROGRESS를 거쳐 APPROVED로 전이된다`() {
        val payment = paymentWith(PaymentStatus.READY)
        payment.transitionTo(PaymentStatus.IN_PROGRESS)
        payment.transitionTo(PaymentStatus.APPROVED)
        assertEquals(PaymentStatus.APPROVED, payment.status)
    }

    @Test
    fun `IN_PROGRESS에서 FAILED로 전이된다`() {
        val payment = paymentWith(PaymentStatus.IN_PROGRESS)
        payment.transitionTo(PaymentStatus.FAILED)
        assertEquals(PaymentStatus.FAILED, payment.status)
    }

    @Test
    fun `APPROVED에서 CANCEL_IN_PROGRESS를 거쳐 CANCELED로 전이된다`() {
        val payment = paymentWith(PaymentStatus.APPROVED)
        payment.transitionTo(PaymentStatus.CANCEL_IN_PROGRESS)
        payment.transitionTo(PaymentStatus.CANCELED)
        assertEquals(PaymentStatus.CANCELED, payment.status)
    }

    @Test
    fun `CANCEL_FAILED에서 CANCEL_IN_PROGRESS로 재시도 전이된다`() {
        val payment = paymentWith(PaymentStatus.CANCEL_FAILED)
        payment.transitionTo(PaymentStatus.CANCEL_IN_PROGRESS)
        assertEquals(PaymentStatus.CANCEL_IN_PROGRESS, payment.status)
    }

    @Test
    fun `APPROVED에서 IN_PROGRESS로 전이하면 예외가 발생한다`() {
        val payment = paymentWith(PaymentStatus.APPROVED)
        assertThrows<IllegalArgumentException> {
            payment.transitionTo(PaymentStatus.IN_PROGRESS)
        }
    }

    @Test
    fun `FAILED에서 IN_PROGRESS로 전이하면 예외가 발생한다`() {
        val payment = paymentWith(PaymentStatus.FAILED)
        assertThrows<IllegalArgumentException> {
            payment.transitionTo(PaymentStatus.IN_PROGRESS)
        }
    }

    @Test
    fun `CANCELED에서 CANCEL_IN_PROGRESS로 전이하면 예외가 발생한다`() {
        val payment = paymentWith(PaymentStatus.CANCELED)
        assertThrows<IllegalArgumentException> {
            payment.transitionTo(PaymentStatus.CANCEL_IN_PROGRESS)
        }
    }

    @Test
    fun `READY에서 APPROVED로 단계를 건너뛰면 예외가 발생한다`() {
        val payment = paymentWith(PaymentStatus.READY)
        assertThrows<IllegalArgumentException> {
            payment.transitionTo(PaymentStatus.APPROVED)
        }
    }
}
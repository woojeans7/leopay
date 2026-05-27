package com.leopay.bookingapi.exception

import com.leopay.core.enums.PaymentStatus

sealed class PaymentException(message: String) : RuntimeException(message) {
    class NotFound(id: Long) : PaymentException("Payment not found: $id")
    class DuplicatePayment(paymentKey: String) : PaymentException("Duplicate payment key: $paymentKey")
    class InvalidStatus(from: PaymentStatus, to: PaymentStatus) : PaymentException("Invalid transition: $from -> $to")
    class LockAcquisitionFailed(paymentKey: String) : PaymentException("Failed to acquire lock: $paymentKey")
    class PgCommunicationFailed(cause: String) : PaymentException("PG communication failed: $cause")
}

package com.leopay.bookingapi.exception

import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

data class ErrorResponse(val code: String, val message: String)

@RestControllerAdvice
class GlobalExceptionHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(PaymentException.NotFound::class)
    fun handleNotFound(e: PaymentException.NotFound): ResponseEntity<ErrorResponse> {
        log.warn("Payment not found: {}", e.message)
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(ErrorResponse("NOT_FOUND", e.message ?: "Not found"))
    }

    @ExceptionHandler(PaymentException.DuplicatePayment::class)
    fun handleDuplicatePayment(e: PaymentException.DuplicatePayment): ResponseEntity<ErrorResponse> {
        log.warn("[A-1] Duplicate payment blocked by distributed lock: {}", e.message)
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(ErrorResponse("DUPLICATE_PAYMENT", e.message ?: "Duplicate payment"))
    }

    @ExceptionHandler(PaymentException.LockAcquisitionFailed::class)
    fun handleLockAcquisitionFailed(e: PaymentException.LockAcquisitionFailed): ResponseEntity<ErrorResponse> {
        log.warn("[A-1] Lock acquisition failed (concurrent request blocked): {}", e.message)
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(ErrorResponse("LOCK_ACQUISITION_FAILED", e.message ?: "Lock acquisition failed"))
    }

    @ExceptionHandler(PaymentException.InvalidStatus::class)
    fun handleInvalidStatus(e: PaymentException.InvalidStatus): ResponseEntity<ErrorResponse> {
        log.warn("Invalid status transition: {}", e.message)
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
            .body(ErrorResponse("INVALID_STATUS", e.message ?: "Invalid status transition"))
    }

    @ExceptionHandler(PaymentException.PgCommunicationFailed::class)
    fun handlePgCommunicationFailed(e: PaymentException.PgCommunicationFailed): ResponseEntity<ErrorResponse> {
        log.warn("[B-1] PG communication failed: {}", e.message)
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
            .body(ErrorResponse("PG_COMMUNICATION_FAILED", e.message ?: "PG communication failed"))
    }

    @ExceptionHandler(DataIntegrityViolationException::class)
    fun handleDataIntegrityViolation(e: DataIntegrityViolationException): ResponseEntity<ErrorResponse> {
        log.error("[A-1] DataIntegrityViolationException (DB unique constraint fallback): {}", e.cause?.message ?: e.message)
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(ErrorResponse("DUPLICATE_PAYMENT", "Duplicate payment key"))
    }

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleMethodArgumentNotValid(e: MethodArgumentNotValidException): ResponseEntity<ErrorResponse> {
        val message = e.bindingResult.fieldErrors.joinToString(", ") { "${it.field}: ${it.defaultMessage}" }
        log.warn("Validation failed: {}", message)
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(ErrorResponse("VALIDATION_FAILED", message))
    }
}

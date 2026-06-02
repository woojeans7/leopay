package com.leopay.bookingapi.controller

import com.leopay.bookingapi.dto.CancelRequest
import com.leopay.bookingapi.dto.PaymentCreateRequest
import com.leopay.bookingapi.dto.PaymentCreateResponse
import com.leopay.bookingapi.dto.PaymentResponse
import com.leopay.bookingapi.idempotency.IdempotencyManager
import com.leopay.bookingapi.service.PaymentService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/payments")
class PaymentController(
    private val paymentService: PaymentService,
    private val idempotencyManager: IdempotencyManager,
) {

    @PostMapping
    fun createPayment(
        @RequestHeader("X-User-Id") userId: String,
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody @Valid request: PaymentCreateRequest,
    ): ResponseEntity<PaymentCreateResponse> {
        val response = if (idempotencyKey != null) {
            idempotencyManager.execute("createPayment", idempotencyKey, PaymentCreateResponse::class.java) {
                paymentService.createPayment(userId, request)
            }
        } else {
            paymentService.createPayment(userId, request)
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(response)
    }

    @PostMapping("/{id}/approve")
    fun approvePayment(
        @PathVariable id: Long,
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
    ): ResponseEntity<PaymentResponse> {
        val response = if (idempotencyKey != null) {
            idempotencyManager.execute("approvePayment", idempotencyKey, PaymentResponse::class.java) {
                paymentService.approvePayment(id)
            }
        } else {
            paymentService.approvePayment(id)
        }
        return ResponseEntity.ok(response)
    }

    @PostMapping("/{id}/cancel")
    fun cancelPayment(
        @PathVariable id: Long,
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody @Valid request: CancelRequest,
    ): ResponseEntity<PaymentResponse> {
        val response = if (idempotencyKey != null) {
            idempotencyManager.execute("cancelPayment", idempotencyKey, PaymentResponse::class.java) {
                paymentService.cancelPayment(id, request)
            }
        } else {
            paymentService.cancelPayment(id, request)
        }
        return ResponseEntity.ok(response)
    }

    @GetMapping("/{id}")
    fun getPayment(
        @PathVariable id: Long,
    ): ResponseEntity<PaymentResponse> {
        val response = paymentService.getPayment(id)
        return ResponseEntity.ok(response)
    }
}

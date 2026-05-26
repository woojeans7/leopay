package com.leopay.core.enums

enum class PaymentStatus(val description: String) {
    READY("결제 생성됨"),
    IN_PROGRESS("PG사 승인 요청 중"),
    APPROVED("PG사 승인 완료"),
    CANCEL_IN_PROGRESS("PG사 취소 요청 중"),
    CANCELED("취소 완료"),
    CANCEL_FAILED("취소 실패"),
    FAILED("승인 실패");

    fun canTransitionTo(next: PaymentStatus): Boolean = next in TRANSITIONS.getOrDefault(this, emptySet())

    companion object {
        private val TRANSITIONS: Map<PaymentStatus, Set<PaymentStatus>> = mapOf(
            READY to setOf(IN_PROGRESS),
            IN_PROGRESS to setOf(APPROVED, FAILED),
            APPROVED to setOf(CANCEL_IN_PROGRESS),
            CANCEL_IN_PROGRESS to setOf(CANCELED, CANCEL_FAILED),
            CANCEL_FAILED to setOf(CANCEL_IN_PROGRESS, APPROVED),
        )
    }
}

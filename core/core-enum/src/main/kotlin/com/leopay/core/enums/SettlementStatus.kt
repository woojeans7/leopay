package com.leopay.core.enums

enum class SettlementStatus(val description: String) {
    PENDING("정산 대기"),
    COMPLETED("정산 완료"),
    TRANSFERRED("송금 완료"),
}

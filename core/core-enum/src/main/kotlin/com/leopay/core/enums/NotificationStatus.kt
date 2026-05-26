package com.leopay.core.enums

enum class NotificationStatus(val description: String) {
    PENDING("발송 대기"),
    SENT("발송 완료"),
    FAILED("발송 실패"),
}

package com.leopay.mockpg.handler

import com.leopay.mockpg.dto.PgApproveRequest
import com.leopay.mockpg.dto.PgApproveResponse
import com.leopay.mockpg.dto.PgCancelRequest
import com.leopay.mockpg.dto.PgCancelResponse
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.awaitBody
import org.springframework.web.reactive.function.server.bodyValueAndAwait
import org.springframework.web.reactive.function.server.buildAndAwait
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import kotlinx.coroutines.delay

// B-1: booking-api WebClient 타임아웃(연결 3초 + 읽기 5초) 검증용 시뮬레이션 핸들러
// ?delay=N (초): N초 지연 후 응답. N>=6 이면 booking-api 읽기 타임아웃(5초) 유발
// ?fail=true : HTTP 500 즉시 반환
@Component
class PgHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun approve(request: ServerRequest): ServerResponse {
        // 강제 실패 시뮬레이션
        if (request.queryParam("fail").orElse("false").toBoolean()) {
            log.warn("[mock-pg] approve forced failure (paymentKey=?)")
            return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .bodyValueAndAwait(mapOf("error" to "PG simulated failure"))
        }

        // 지연 시뮬레이션
        val delaySeconds = request.queryParam("delay").orElse("0").toLongOrNull() ?: 0L
        if (delaySeconds > 0) {
            log.info("[mock-pg] approve delay {}s", delaySeconds)
            delay(Duration.ofSeconds(delaySeconds).toMillis())
        }

        val body = request.awaitBody<PgApproveRequest>()
        log.info("[mock-pg] approve paymentKey={} amount={} method={}", body.paymentKey, body.amount, body.method)

        val response = PgApproveResponse(
            pgTransactionId = "PG-${UUID.randomUUID()}",
            approvedAt = LocalDateTime.now(),
        )
        return ServerResponse.ok().bodyValueAndAwait(response)
    }

    suspend fun cancel(request: ServerRequest): ServerResponse {
        // 강제 실패 시뮬레이션
        if (request.queryParam("fail").orElse("false").toBoolean()) {
            log.warn("[mock-pg] cancel forced failure")
            return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .bodyValueAndAwait(mapOf("error" to "PG simulated failure"))
        }

        // 지연 시뮬레이션
        val delaySeconds = request.queryParam("delay").orElse("0").toLongOrNull() ?: 0L
        if (delaySeconds > 0) {
            log.info("[mock-pg] cancel delay {}s", delaySeconds)
            delay(Duration.ofSeconds(delaySeconds).toMillis())
        }

        val body = request.awaitBody<PgCancelRequest>()
        log.info("[mock-pg] cancel pgTransactionId={} reason={}", body.pgTransactionId, body.cancelReason)

        val response = PgCancelResponse(canceledAt = LocalDateTime.now())
        return ServerResponse.ok().bodyValueAndAwait(response)
    }

    @Suppress("unused")
    suspend fun health(request: ServerRequest): ServerResponse =
        ServerResponse.ok().bodyValueAndAwait(mapOf("status" to "UP"))
}
package com.leopay.mockpg.handler

import com.leopay.mockpg.dto.PgApproveRequest
import com.leopay.mockpg.dto.PgCancelRequest
import com.leopay.mockpg.router.PgRouter
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import java.math.BigDecimal

@WebFluxTest
@Import(PgRouter::class, PgHandler::class)
class PgHandlerTest {

    @Autowired
    private lateinit var client: WebTestClient

    @Test
    fun `POST pg approve - 정상 응답`() {
        val request = PgApproveRequest(
            paymentKey = "pay-test-001",
            amount = BigDecimal("10000"),
            method = "CARD",
        )

        client.post().uri("/pg/approve")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { result ->
                val body = result.responseBody!!
                assert(body.containsKey("pgTransactionId")) { "pgTransactionId 필드 없음" }
                assert(body.containsKey("approvedAt")) { "approvedAt 필드 없음" }
                val txId = body["pgTransactionId"] as String
                assert(txId.startsWith("PG-")) { "pgTransactionId 형식 오류: $txId" }
            }
    }

    @Test
    fun `POST pg approve - fail=true 이면 500 반환`() {
        val request = PgApproveRequest(
            paymentKey = "pay-test-fail",
            amount = BigDecimal("10000"),
            method = "CARD",
        )

        client.post().uri("/pg/approve?fail=true")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `POST pg cancel - 정상 응답`() {
        val request = PgCancelRequest(
            pgTransactionId = "PG-test-tx-001",
            cancelReason = "고객 요청 취소",
        )

        client.post().uri("/pg/cancel")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { result ->
                val body = result.responseBody!!
                assert(body.containsKey("canceledAt")) { "canceledAt 필드 없음" }
            }
    }

    @Test
    fun `POST pg cancel - fail=true 이면 500 반환`() {
        val request = PgCancelRequest(
            pgTransactionId = "PG-test-tx-fail",
            cancelReason = "테스트 실패 시뮬레이션",
        )

        client.post().uri("/pg/cancel?fail=true")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `GET pg health - 상태 확인`() {
        client.get().uri("/pg/health")
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { result ->
                val body = result.responseBody!!
                assert(body["status"] == "UP") { "health status 오류: ${body["status"]}" }
            }
    }
}
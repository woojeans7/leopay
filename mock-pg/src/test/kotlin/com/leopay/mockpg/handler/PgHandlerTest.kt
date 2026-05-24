package com.leopay.mockpg.handler

import com.leopay.mockpg.dto.PgApproveRequest
import com.leopay.mockpg.dto.PgCancelRequest
import com.leopay.mockpg.router.PgRouter
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.returnResult
import java.math.BigDecimal

/**
 * PgHandler 단위 테스트
 *
 * WebTestClient.bindToRouterFunction()으로 Spring 컨텍스트 없이 핸들러 로직을 검증한다.
 */
class PgHandlerTest {

    private lateinit var webTestClient: WebTestClient

    @BeforeEach
    fun setUp() {
        val handler = PgHandler()
        val router = PgRouter()
        webTestClient = WebTestClient
            .bindToRouterFunction(router.pgRoutes(handler))
            .build()
    }

    private fun approveRequest() = PgApproveRequest(
        orderId = "ORDER-TEST-001",
        amount = BigDecimal("10000"),
        method = "CARD",
    )

    private fun cancelRequest() = PgCancelRequest(
        transactionId = "TX-001",
        amount = BigDecimal("10000"),
        reason = null,
    )

    @Suppress("UNCHECKED_CAST")
    private fun postApprove(uri: String = "/pg/approve"): Map<String, Any?> =
        webTestClient.post()
            .uri(uri)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(approveRequest())
            .exchange()
            .expectStatus().isOk
            .returnResult<Map<String, Any?>>()
            .responseBody.blockFirst()!!

    @Suppress("UNCHECKED_CAST")
    private fun postCancel(): Map<String, Any?> =
        webTestClient.post()
            .uri("/pg/cancel")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(cancelRequest())
            .exchange()
            .expectStatus().isOk
            .returnResult<Map<String, Any?>>()
            .responseBody.blockFirst()!!

    @Nested
    @DisplayName("POST /pg/approve")
    inner class Approve {

        @Test
        @DisplayName("항상 HTTP 200 응답 반환")
        fun `approve 요청은 항상 200 응답`() {
            webTestClient.post()
                .uri("/pg/approve")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(approveRequest())
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
        }

        @Test
        @DisplayName("success=true 시 transactionId, approvedAt 포함 / success=false 시 errorMessage 포함")
        fun `approve 응답 필드 검증`() {
            var foundSuccess = false
            var foundFailure = false

            repeat(100) {
                if (foundSuccess && foundFailure) return@repeat
                val body = postApprove()
                if (body["success"] == true && !foundSuccess) {
                    assertThat(body["transactionId"]).isNotNull()
                    assertThat(body["approvedAt"]).isNotNull()
                    assertThat(body["errorMessage"]).isNull()
                    foundSuccess = true
                }
                if (body["success"] == false && !foundFailure) {
                    assertThat(body["errorMessage"]).isNotNull()
                    assertThat(body["transactionId"]).isNull()
                    foundFailure = true
                }
            }

            assertThat(foundSuccess).`as`("100회 중 성공 응답을 찾지 못했습니다 (예상 성공률 90%)").isTrue()
            assertThat(foundFailure).`as`("100회 중 실패 응답을 찾지 못했습니다 (예상 실패율 10%)").isTrue()
        }

        @Test
        @DisplayName("100회 중 성공률이 70~100% 범위")
        fun `approve 성공률 통계 검증`() {
            val results = (1..100).map { postApprove() }
            val successCount = results.count { it["success"] == true }
            assertThat(successCount)
                .`as`("100회 중 성공 건수가 70~100 범위여야 합니다 (실제: $successCount)")
                .isBetween(70, 100)
        }

        @Test
        @DisplayName("delay 파라미터 적용 시 지연 후 응답")
        fun `delay 파라미터 적용 시 지연 응답`() {
            val delayMs = 200L
            val start = System.nanoTime()

            postApprove("/pg/approve?delay=$delayMs")

            val elapsedMs = (System.nanoTime() - start) / 1_000_000
            assertThat(elapsedMs)
                .`as`("delay=${delayMs}ms 설정 시 응답 시간이 ${delayMs}ms 이상이어야 합니다 (실제: ${elapsedMs}ms)")
                .isGreaterThanOrEqualTo(delayMs - 50)
        }
    }

    @Nested
    @DisplayName("POST /pg/cancel")
    inner class Cancel {

        @Test
        @DisplayName("항상 HTTP 200 응답 반환")
        fun `cancel 요청은 항상 200 응답`() {
            webTestClient.post()
                .uri("/pg/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(cancelRequest())
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
        }

        @Test
        @DisplayName("100회 중 성공률이 80~100% 범위")
        fun `cancel 성공률 통계 검증`() {
            val results = (1..100).map { postCancel() }
            val successCount = results.count { it["success"] == true }
            assertThat(successCount)
                .`as`("100회 중 성공 건수가 80~100 범위여야 합니다 (실제: $successCount)")
                .isBetween(80, 100)
        }

        @Test
        @DisplayName("success=true 시 canceledAt 포함")
        fun `cancel 성공 응답에 canceledAt 포함`() {
            var foundSuccess = false
            repeat(100) {
                if (foundSuccess) return@repeat
                val body = postCancel()
                if (body["success"] == true) {
                    assertThat(body["canceledAt"]).isNotNull()
                    foundSuccess = true
                }
            }
            assertThat(foundSuccess).`as`("100회 중 성공 응답을 찾지 못했습니다 (예상 성공률 95%)").isTrue()
        }
    }
}

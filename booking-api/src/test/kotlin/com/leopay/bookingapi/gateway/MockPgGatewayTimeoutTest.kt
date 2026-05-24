package com.leopay.bookingapi.gateway

import com.leopay.bookingapi.config.PgTimeoutResolver
import com.leopay.bookingapi.gateway.dto.PgApproveRequest
import com.leopay.bookingapi.gateway.dto.PgCancelRequest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient
import java.math.BigDecimal
import java.util.concurrent.TimeUnit

/**
 * MockPgGateway WebClient 타임아웃 단위 테스트
 *
 * MockWebServer로 지연 응답을 재현하고,
 * PgTimeoutResolver를 직접 생성해 타임아웃 값을 1초로 단축하여 실제 타임아웃 로직을 검증한다.
 */
class MockPgGatewayTimeoutTest {

    private lateinit var mockWebServer: MockWebServer

    @BeforeEach
    fun setUp() {
        mockWebServer = MockWebServer()
        mockWebServer.start()
    }

    @AfterEach
    fun tearDown() {
        mockWebServer.shutdown()
    }

    private class FixedTimeoutResolver(override val seconds: Long) : PgTimeoutResolver("normal")

    private fun createGateway(timeoutSeconds: Long): MockPgGateway {
        val webClient = WebClient.builder()
            .baseUrl(mockWebServer.url("/").toString())
            .build()
        return MockPgGateway(webClient, FixedTimeoutResolver(timeoutSeconds))
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

    @Nested
    @DisplayName("approve() 타임아웃")
    inner class ApproveTimeout {

        @Test
        @DisplayName("응답이 타임아웃(1초) 초과 시 → success=false, errorMessage에 timeout 포함")
        fun `응답 지연이 타임아웃 초과 시 실패 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse()
                    .setBody("""{"success":true,"transactionId":"tx-ok"}""")
                    .addHeader("Content-Type", "application/json")
                    .setBodyDelay(2, TimeUnit.SECONDS)
            )

            val result = createGateway(timeoutSeconds = 1L).approve(approveRequest())

            assertThat(result.success).isFalse()
            assertThat(result.errorMessage).isEqualTo("PG timeout: 1초 초과")
        }

        @Test
        @DisplayName("응답이 타임아웃 이내 도착 시 → success=true, transactionId 정상 반환")
        fun `응답이 타임아웃 이내 도착 시 정상 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse()
                    .setBody("""{"success":true,"transactionId":"tx-ok-123"}""")
                    .addHeader("Content-Type", "application/json")
            )

            val result = createGateway(timeoutSeconds = 5L).approve(approveRequest())

            assertThat(result.success).isTrue()
            assertThat(result.transactionId).isEqualTo("tx-ok-123")
        }
    }

    @Nested
    @DisplayName("cancel() 타임아웃")
    inner class CancelTimeout {

        @Test
        @DisplayName("응답이 타임아웃(1초) 초과 시 → success=false, errorMessage에 timeout 포함")
        fun `cancel 응답 지연이 타임아웃 초과 시 실패 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse()
                    .setBody("""{"success":true}""")
                    .addHeader("Content-Type", "application/json")
                    .setBodyDelay(2, TimeUnit.SECONDS)
            )

            val result = createGateway(timeoutSeconds = 1L).cancel(cancelRequest())

            assertThat(result.success).isFalse()
            assertThat(result.errorMessage).isEqualTo("PG timeout: 1초 초과")
        }

        @Test
        @DisplayName("응답이 타임아웃 이내 도착 시 → success=true")
        fun `cancel 응답이 타임아웃 이내 도착 시 정상 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse()
                    .setBody("""{"success":true}""")
                    .addHeader("Content-Type", "application/json")
            )

            val result = createGateway(timeoutSeconds = 5L).cancel(cancelRequest())

            assertThat(result.success).isTrue()
        }
    }

    @Nested
    @DisplayName("오류 응답 처리")
    inner class ErrorHandling {

        @Test
        @DisplayName("PG 서버가 HTTP 500 응답 시 → success=false, errorMessage에 HTTP 500 포함")
        fun `HTTP 500 응답 시 오류 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse()
                    .setResponseCode(500)
                    .addHeader("Content-Type", "application/json")
            )

            val result = createGateway(timeoutSeconds = 5L).approve(approveRequest())

            assertThat(result.success).isFalse()
            assertThat(result.errorMessage).contains("HTTP 500")
        }

        @Test
        @DisplayName("연결이 즉시 끊길 시 → success=false, errorMessage에 연결 실패 포함")
        fun `연결 실패 시 오류 응답 반환`() {
            mockWebServer.enqueue(
                MockResponse().setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.DISCONNECT_AT_START)
            )

            val result = createGateway(timeoutSeconds = 5L).approve(approveRequest())

            assertThat(result.success).isFalse()
            assertThat(result.errorMessage).contains("연결 실패")
        }
    }

    @Nested
    @DisplayName("load-level별 타임아웃 설정값")
    inner class LoadLevelTimeout {

        @Test
        @DisplayName("normal → 10초")
        fun `normal load-level 타임아웃은 10초`() {
            assertThat(PgTimeoutResolver("normal").seconds).isEqualTo(10L)
        }

        @Test
        @DisplayName("peak → 20초")
        fun `peak load-level 타임아웃은 20초`() {
            assertThat(PgTimeoutResolver("peak").seconds).isEqualTo(20L)
        }

        @Test
        @DisplayName("알 수 없는 값 → 10초 (기본값)")
        fun `알 수 없는 load-level은 기본값 10초`() {
            assertThat(PgTimeoutResolver("unknown").seconds).isEqualTo(10L)
        }
    }
}

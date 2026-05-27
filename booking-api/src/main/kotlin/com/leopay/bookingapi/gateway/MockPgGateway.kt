package com.leopay.bookingapi.gateway

import com.leopay.bookingapi.exception.PaymentException
import com.leopay.bookingapi.gateway.dto.PgApproveRequest
import com.leopay.bookingapi.gateway.dto.PgApproveResponse
import com.leopay.bookingapi.gateway.dto.PgCancelRequest
import com.leopay.bookingapi.gateway.dto.PgCancelResponse
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeoutException

@Component
@Primary
class MockPgGateway(
    private val pgWebClient: WebClient,
    private val timeoutResolver: PgTimeoutResolver,
) : PaymentGateway {

    override suspend fun approve(request: PgApproveRequest): PgApproveResponse {
        return pgWebClient.post()
            .uri("/pg/approve")
            .bodyValue(request)
            .retrieve()
            .bodyToMono(PgApproveResponse::class.java)
            .timeout(Duration.ofSeconds(timeoutResolver.seconds))
            .onErrorMap(WebClientRequestException::class.java) { e ->
                PaymentException.PgCommunicationFailed(e.message ?: "WebClient request error")
            }
            .onErrorMap(TimeoutException::class.java) { _ ->
                PaymentException.PgCommunicationFailed("PG response timeout after ${timeoutResolver.seconds}s")
            }
            .onErrorMap { e ->
                if (e is PaymentException) e
                else PaymentException.PgCommunicationFailed(e.message ?: "Unknown PG error")
            }
            .flatMap { response ->
                if (response != null) Mono.just(response)
                else Mono.error(PaymentException.PgCommunicationFailed("Empty response from PG"))
            }
            .awaitSingle()
    }

    override suspend fun cancel(request: PgCancelRequest): PgCancelResponse {
        return pgWebClient.post()
            .uri("/pg/cancel")
            .bodyValue(request)
            .retrieve()
            .bodyToMono(PgCancelResponse::class.java)
            .timeout(Duration.ofSeconds(timeoutResolver.seconds))
            .onErrorMap(WebClientRequestException::class.java) { e ->
                PaymentException.PgCommunicationFailed(e.message ?: "WebClient request error")
            }
            .onErrorMap(TimeoutException::class.java) { _ ->
                PaymentException.PgCommunicationFailed("PG response timeout after ${timeoutResolver.seconds}s")
            }
            .onErrorMap { e ->
                if (e is PaymentException) e
                else PaymentException.PgCommunicationFailed(e.message ?: "Unknown PG error")
            }
            .flatMap { response ->
                if (response != null) Mono.just(response)
                else Mono.error(PaymentException.PgCommunicationFailed("Empty response from PG"))
            }
            .awaitSingle()
    }
}

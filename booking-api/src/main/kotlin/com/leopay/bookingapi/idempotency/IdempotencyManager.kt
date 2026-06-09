package com.leopay.bookingapi.idempotency

import com.fasterxml.jackson.databind.ObjectMapper
import org.redisson.api.RedissonClient
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * A-2: 멱등성 처리 컴포넌트
 *
 * Redis에 (idempotency-key → JSON 직렬화 응답)을 저장한다.
 * 동일 키로 재요청이 오면 서비스 로직을 실행하지 않고 저장된 응답을 그대로 반환한다.
 *
 * 이중 레이어:
 *   1차 방어: Redis (TTL 24시간) — 재시도 중복 실행 차단
 *   2차 방어: DB unique constraint (payment_key) — Redis 장애·TTL 만료 후 최후 방어선
 *
 * 키 형식: idempotency:{userId}:{apiName}:{idempotencyKey}
 */
@Component
class IdempotencyManager(
    private val redissonClient: RedissonClient,
    private val objectMapper: ObjectMapper,
) {

    companion object {
        private const val KEY_PREFIX = "idempotency"
        private val TTL = Duration.ofHours(24)
    }

    /**
     * 멱등성 체크 후 서비스 로직을 실행한다.
     *
     * @param userId     X-User-Id 헤더 값 (키 충돌 방지를 위해 포함)
     * @param apiName    API 이름 (예: "createPayment")
     * @param idempotencyKey  클라이언트가 전송한 Idempotency-Key 헤더 값
     * @param responseClass  응답 역직렬화에 사용할 클래스
     * @param block      실제 서비스 로직
     * @return 기존 응답(재시도) 또는 신규 응답(최초 요청)
     */
    fun <T : Any> execute(
        userId: String,
        apiName: String,
        idempotencyKey: String,
        responseClass: Class<T>,
        block: () -> T,
    ): T {
        val redisKey = "$KEY_PREFIX:$userId:$apiName:$idempotencyKey"
        val bucket = redissonClient.getBucket<String>(redisKey)

        // 1차 방어: Redis에 저장된 응답이 있으면 그대로 반환
        val cached = bucket.get()
        if (cached != null) {
            return objectMapper.readValue(cached, responseClass)
        }

        // 최초 요청: 서비스 로직 실행 후 결과를 Redis에 저장
        val response = block()
        bucket.set(objectMapper.writeValueAsString(response), TTL)
        return response
    }
}

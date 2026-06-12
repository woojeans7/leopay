package com.leopay.bookingapi.idempotency

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.leopay.bookingapi.dto.PaymentCreateResponse
import com.leopay.core.enums.PaymentStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.redisson.api.RBucket
import org.redisson.api.RedissonClient
import java.time.Duration

class IdempotencyManagerTest {

    private lateinit var redissonClient: RedissonClient
    private lateinit var idempotencyManager: IdempotencyManager
    private val objectMapper = ObjectMapper().registerKotlinModule()

    @BeforeEach
    fun setUp() {
        redissonClient = mockk()
        idempotencyManager = IdempotencyManager(redissonClient, objectMapper)
    }

    @Test
    fun `최초 요청 시 block이 실행되고 결과가 Redis에 저장된다`() {
        val bucket = mockk<RBucket<String>>(relaxed = true)
        every { redissonClient.getBucket<String>("idempotency:user1:createPayment:key-001") } returns bucket
        every { bucket.get() } returns null

        val expectedResponse = PaymentCreateResponse(
            paymentId = 1L,
            paymentKey = "pay-001",
            status = PaymentStatus.READY,
        )
        var blockCallCount = 0

        val result = idempotencyManager.execute(
            userId = "user1",
            apiName = "createPayment",
            idempotencyKey = "key-001",
            responseClass = PaymentCreateResponse::class.java,
        ) {
            blockCallCount++
            expectedResponse
        }

        assertEquals(1, blockCallCount, "최초 요청에서 block은 1회 실행되어야 한다")
        assertEquals(expectedResponse, result)
        verify(exactly = 1) { bucket.set(any<String>(), Duration.ofHours(24)) }
    }

    @Test
    fun `동일 userId와 동일 key로 재시도하면 block이 실행되지 않고 캐시된 응답이 반환된다`() {
        val bucket = mockk<RBucket<String>>(relaxed = true)
        every { redissonClient.getBucket<String>("idempotency:user1:createPayment:key-001") } returns bucket

        val cachedResponse = PaymentCreateResponse(
            paymentId = 1L,
            paymentKey = "pay-001",
            status = PaymentStatus.READY,
        )
        every { bucket.get() } returns objectMapper.writeValueAsString(cachedResponse)

        var blockCallCount = 0

        val result = idempotencyManager.execute(
            userId = "user1",
            apiName = "createPayment",
            idempotencyKey = "key-001",
            responseClass = PaymentCreateResponse::class.java,
        ) {
            blockCallCount++
            PaymentCreateResponse(paymentId = 99L, paymentKey = "should-not-appear", status = PaymentStatus.FAILED)
        }

        assertEquals(0, blockCallCount, "재시도 시 block은 실행되지 않아야 한다")
        assertEquals(cachedResponse.paymentId, result.paymentId)
        assertEquals(cachedResponse.paymentKey, result.paymentKey)
        verify(exactly = 0) { bucket.set(any<String>(), any<Duration>()) }
    }

    @Test
    fun `다른 userId는 동일 idempotencyKey를 사용해도 독립적으로 처리된다`() {
        val user1Bucket = mockk<RBucket<String>>(relaxed = true)
        val user2Bucket = mockk<RBucket<String>>(relaxed = true)

        val user1CachedResponse = PaymentCreateResponse(
            paymentId = 1L,
            paymentKey = "pay-user1",
            status = PaymentStatus.APPROVED,
        )
        every { redissonClient.getBucket<String>("idempotency:user1:createPayment:key-shared") } returns user1Bucket
        every { user1Bucket.get() } returns objectMapper.writeValueAsString(user1CachedResponse)

        every { redissonClient.getBucket<String>("idempotency:user2:createPayment:key-shared") } returns user2Bucket
        every { user2Bucket.get() } returns null

        val user2Response = PaymentCreateResponse(
            paymentId = 2L,
            paymentKey = "pay-user2",
            status = PaymentStatus.READY,
        )
        var user2BlockCallCount = 0

        val result = idempotencyManager.execute(
            userId = "user2",
            apiName = "createPayment",
            idempotencyKey = "key-shared",
            responseClass = PaymentCreateResponse::class.java,
        ) {
            user2BlockCallCount++
            user2Response
        }

        assertEquals(1, user2BlockCallCount, "user2의 block은 1회 실행되어야 한다")
        assertEquals(user2Response.paymentId, result.paymentId, "user2는 user1과 독립적인 응답을 받아야 한다")
        verify(exactly = 1) { user2Bucket.set(any<String>(), Duration.ofHours(24)) }
        verify(exactly = 0) { user1Bucket.set(any<String>(), any<Duration>()) }
    }
}

package com.leopay.bookingapi.lock

import com.leopay.bookingapi.exception.PaymentException
import org.redisson.api.RedissonClient
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class RedisLockManager(
    private val redissonClient: RedissonClient,
) : LockManager {
    override fun <T> withLock(paymentKey: String, block: () -> T): T =
        withLockByKey("lock:payment:$paymentKey", paymentKey, block)

    override fun <T> withLock(prefix: String, key: String, block: () -> T): T =
        withLockByKey("$prefix:$key", key, block)

    private fun <T> withLockByKey(lockKey: String, displayKey: String, block: () -> T): T {
        val lock = redissonClient.getLock(lockKey)
        val acquired = lock.tryLock(3, 10, TimeUnit.SECONDS)
        if (!acquired) {
            throw PaymentException.LockAcquisitionFailed(displayKey)
        }
        try {
            return block()
        } finally {
            if (lock.isHeldByCurrentThread) {
                lock.unlock()
            }
        }
    }
}

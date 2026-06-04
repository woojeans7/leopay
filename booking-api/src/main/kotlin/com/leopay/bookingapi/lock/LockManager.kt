package com.leopay.bookingapi.lock

interface LockManager {
    fun <T> withLock(paymentKey: String, block: () -> T): T
    fun <T> withLock(prefix: String, key: String, block: () -> T): T
}

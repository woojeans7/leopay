package com.leopay.mockpg

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MockPgApplication

fun main(args: Array<String>) {
    runApplication<MockPgApplication>(*args)
}
package com.leopay.bookingapi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class BookingApiApplication

fun main(args: Array<String>) {
    runApplication<BookingApiApplication>(*args)
}

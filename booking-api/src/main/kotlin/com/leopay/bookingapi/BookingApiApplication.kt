package com.leopay.bookingapi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = ["com.leopay.storage.entity"])
@EnableJpaRepositories(basePackages = ["com.leopay.storage.repository"])
class BookingApiApplication

fun main(args: Array<String>) {
    runApplication<BookingApiApplication>(*args)
}

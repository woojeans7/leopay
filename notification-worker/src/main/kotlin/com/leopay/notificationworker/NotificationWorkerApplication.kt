package com.leopay.notificationworker

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.data.jpa.repository.config.EnableJpaAuditing

@SpringBootApplication
@EnableJpaAuditing
@EntityScan(basePackages = ["com.leopay.storage.entity"])
@EnableJpaRepositories(basePackages = ["com.leopay.storage.repository"])
class NotificationWorkerApplication

fun main(args: Array<String>) {
    runApplication<NotificationWorkerApplication>(*args)
}

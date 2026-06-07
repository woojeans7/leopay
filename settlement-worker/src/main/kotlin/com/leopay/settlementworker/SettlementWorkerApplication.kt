package com.leopay.settlementworker

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaAuditing
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableJpaAuditing
@EntityScan(basePackages = ["com.leopay.storage.entity"])
@EnableJpaRepositories(basePackages = ["com.leopay.storage.repository"])
@EnableScheduling
class SettlementWorkerApplication

fun main(args: Array<String>) {
    runApplication<SettlementWorkerApplication>(*args)
}

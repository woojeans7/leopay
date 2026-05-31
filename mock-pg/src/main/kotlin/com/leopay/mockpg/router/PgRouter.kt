package com.leopay.mockpg.router

import com.leopay.mockpg.handler.PgHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.coRouter

@Configuration
class PgRouter {

    @Bean
    fun pgRoutes(handler: PgHandler) = coRouter {
        "/pg".nest {
            POST("/approve", handler::approve)
            POST("/cancel", handler::cancel)
            GET("/health", handler::health)
        }
    }
}
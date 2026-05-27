package com.leopay.bookingapi.config

import io.netty.channel.ChannelOption
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient

@Configuration
class WebClientConfig {

    @Value("\${pg.base-url}")
    private lateinit var pgBaseUrl: String

    @Bean
    fun pgWebClient(): WebClient {
        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3_000)

        return WebClient.builder()
            .baseUrl(pgBaseUrl)
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .build()
    }
}
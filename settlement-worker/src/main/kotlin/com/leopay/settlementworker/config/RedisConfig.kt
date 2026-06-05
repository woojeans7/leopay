package com.leopay.settlementworker.config

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * B-3: 컨슈머 중복 소비 방지를 위한 Redis 설정
 *
 * RedissonClient 를 이용해 settlement:processed:{paymentId} 키로
 * 동일 이벤트의 중복 처리 여부를 확인한다.
 */
@Configuration
class RedisConfig {

    @Value("\${spring.data.redis.host}")
    private lateinit var host: String

    @Value("\${spring.data.redis.port}")
    private var port: Int = 6379

    @Bean
    fun redissonClient(): RedissonClient {
        val config = Config()
        config.useSingleServer().address = "redis://$host:$port"
        return Redisson.create(config)
    }
}

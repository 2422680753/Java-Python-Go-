package com.riskcontrol.gateway.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class Resilience4jConfig {

    private static final Logger logger = LoggerFactory.getLogger(Resilience4jConfig.class);

    private final Map<String, CircuitBreaker> circuitBreakerCache = new ConcurrentHashMap<>();
    private final Map<String, RateLimiter> rateLimiterCache = new ConcurrentHashMap<>();

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig defaultConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(60)
                .slowCallDurationThreshold(Duration.ofSeconds(2))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(defaultConfig);

        registry.getEventPublisher()
                .onEntryAdded(event -> logger.info("CircuitBreaker created: {}", event.getEntry().getName()));

        return registry;
    }

    @Bean
    public RateLimiterRegistry rateLimiterRegistry() {
        RateLimiterConfig defaultConfig = RateLimiterConfig.custom()
                .limitForPeriod(1000)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(0))
                .build();

        RateLimiterRegistry registry = RateLimiterRegistry.of(defaultConfig);

        return registry;
    }

    @Bean
    public TimeLimiterRegistry timeLimiterRegistry() {
        TimeLimiterConfig defaultConfig = TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(500))
                .cancelRunningFuture(true)
                .build();

        return TimeLimiterRegistry.of(defaultConfig);
    }

    public CircuitBreaker getTransactionGatewayCircuitBreaker(CircuitBreakerRegistry registry) {
        return circuitBreakerCache.computeIfAbsent("transaction-gateway", 
            name -> registry.circuitBreaker(name, createGatewayCircuitBreakerConfig()));
    }

    public CircuitBreaker getRulesEngineCircuitBreaker(CircuitBreakerRegistry registry) {
        return circuitBreakerCache.computeIfAbsent("rules-engine", 
            name -> registry.circuitBreaker(name, createRulesEngineCircuitBreakerConfig()));
    }

    public CircuitBreaker getMlServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return circuitBreakerCache.computeIfAbsent("ml-service", 
            name -> registry.circuitBreaker(name, createMlServiceCircuitBreakerConfig()));
    }

    public RateLimiter getTransactionRateLimiter(RateLimiterRegistry registry) {
        return rateLimiterCache.computeIfAbsent("transaction-rate", 
            name -> registry.rateLimiter(name, createTransactionRateLimiterConfig()));
    }

    public RateLimiter getHeavyOperationRateLimiter(RateLimiterRegistry registry) {
        return rateLimiterCache.computeIfAbsent("heavy-operation", 
            name -> registry.rateLimiter(name, createHeavyOperationRateLimiterConfig()));
    }

    private CircuitBreakerConfig createGatewayCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(40)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(200)
                .minimumNumberOfCalls(20)
                .waitDurationInOpenState(Duration.ofSeconds(15))
                .build();
    }

    private CircuitBreakerConfig createRulesEngineCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(30)
                .slowCallRateThreshold(40)
                .slowCallDurationThreshold(Duration.ofMillis(500))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build();
    }

    private CircuitBreakerConfig createMlServiceCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(25)
                .slowCallRateThreshold(35)
                .slowCallDurationThreshold(Duration.ofMillis(2000))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(50)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofSeconds(60))
                .build();
    }

    private RateLimiterConfig createTransactionRateLimiterConfig() {
        return RateLimiterConfig.custom()
                .limitForPeriod(5000)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(0))
                .build();
    }

    private RateLimiterConfig createHeavyOperationRateLimiterConfig() {
        return RateLimiterConfig.custom()
                .limitForPeriod(100)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(100))
                .build();
    }
}

package com.ingestor.services;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.Callable;

@Service
public class KafkaRetryService {
    private final Logger log = LoggerFactory.getLogger(KafkaRetryService.class);
    private final Retry retry;
    private final CircuitBreaker circuitBreaker;

    public KafkaRetryService(RetryRegistry retryRegistry, CircuitBreakerRegistry circuitBreakerRegistry) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                // Exponential backoff
                .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(1), 2))
                .retryExceptions(KafkaException.class)
                .build();

        this.retry = retryRegistry.retry("kafkaRetry", config);
        this.retry.getEventPublisher()
                .onRetry(event -> log.info("Retry #{}", event.getNumberOfRetryAttempts()))
                .onError(event -> log.info("Error retry: {}", event.getLastThrowable().getMessage()));

        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .build();

        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("kafkaCircuitBreaker", cbConfig);

        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> log.info("CircuitBreaker state changed: " + event.getStateTransition()));
    }

    public <T> void executeWithRetry(Callable<T> callable) throws Exception {
        Retry.decorateCallable(retry, callable).call();
    }

    public <T> void executeResilientWithRetry(Callable<T> callable) throws Exception {
        Callable<T> decorated = CircuitBreaker.decorateCallable(circuitBreaker, Retry.decorateCallable(retry, callable));
        decorated.call();
    }
}

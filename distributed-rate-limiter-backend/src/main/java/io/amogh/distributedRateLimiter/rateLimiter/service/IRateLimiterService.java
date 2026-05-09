package io.amogh.distributedRateLimiter.rateLimiter.service;

import reactor.core.publisher.Mono;

public interface IRateLimiterService {

    String REDIS_KEY_PREFIX = "rate-limiter:";

    Mono<Boolean> tryConsume(String userId);
    Mono<Integer> getRemaining(String userId);
    Mono<Integer> tryConsumeAndGetRemaining(String userId);

}

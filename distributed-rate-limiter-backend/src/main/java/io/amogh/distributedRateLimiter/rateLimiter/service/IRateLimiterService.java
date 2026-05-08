package io.amogh.distributedRateLimiter.rateLimiter.service;

public interface IRateLimiterService {

    String REDIS_KEY_PREFIX = "rate-limiter:";

    boolean tryConsume(String userId);
    int getRemaining(String userId);
    int tryConsumeAndGetRemaining(String userId);

}

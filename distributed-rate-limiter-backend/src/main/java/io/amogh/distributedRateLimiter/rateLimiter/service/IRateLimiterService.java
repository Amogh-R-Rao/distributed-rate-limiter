package io.amogh.distributedRateLimiter.rateLimiter.service;

public interface IRateLimiterService {

    boolean  tryConsume(String userId);
    int getRemaining(String userId);
    int tryConsumeAndGetRemaining(String userId);

}

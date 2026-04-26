package io.amogh.distributedRateLimiter.rateLimiter.service;

interface IRateLimiterService {
    boolean increment();
    int getCount();
    int incrementAndGetCount();
}

package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.BucketRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("token-bucket")
@RequiredArgsConstructor
public class TokenBucketRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final BucketRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId) >= 0;
    }

    @Override
    public int getRemaining(String userId) {
        String userTokenKey = REDIS_KEY_PREFIX + "tb:tokens:" + userId;
        String userRefillKey = REDIS_KEY_PREFIX + "tb:last-refill:" + userId;

        long now = Instant.now().getEpochSecond();
        long capacity = properties.getMaxTokens();
        long refillRate = properties.getRefillAmount();
        long refillInterval = properties.getRefillRate();

        String currentVal = redisTemplate.opsForValue().get(userTokenKey);
        long currentCount = Objects.nonNull(currentVal) ? Long.parseLong(currentVal) : capacity;

        String refillVal = redisTemplate.opsForValue().get(userRefillKey);
        long lastRefill = Objects.nonNull(refillVal) ? Long.parseLong(refillVal) : now;

        long elapsed = now - lastRefill;
        if (elapsed < 0) elapsed = 0;
        long tokenToAdd = (refillRate * elapsed) / refillInterval;
        currentCount = Math.min(capacity, currentCount + tokenToAdd);

        return (int) Math.max(0, currentCount);
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        String userTokenKey = REDIS_KEY_PREFIX + "tb:tokens:" + userId;
        String userRefillKey = REDIS_KEY_PREFIX + "tb:last-refill:" + userId;

        long now = Instant.now().getEpochSecond();
        long capacity = properties.getMaxTokens();
        long refillRate = properties.getRefillAmount();
        long refillInterval = properties.getRefillRate();
        long ttl = refillInterval * 2;

        String currentVal = redisTemplate.opsForValue().get(userTokenKey);
        long currentCount = Objects.nonNull(currentVal) ? Long.parseLong(currentVal) : capacity;

        String refillVal = redisTemplate.opsForValue().get(userRefillKey);
        long lastRefill = Objects.nonNull(refillVal) ? Long.parseLong(refillVal) : now;

        long elapsed = now - lastRefill;
        if (elapsed < 0) elapsed = 0;
        long tokenToAdd = (refillRate * elapsed) / refillInterval;
        currentCount = Math.min(capacity, currentCount + tokenToAdd);

        if (currentCount >= 1) {
            currentCount--;
            redisTemplate.opsForValue().set(userTokenKey, String.valueOf(currentCount), Duration.ofSeconds(ttl));
            redisTemplate.opsForValue().set(userRefillKey, String.valueOf(now), Duration.ofSeconds(ttl));
            return (int) currentCount;
        }

        return -1;
    }

}

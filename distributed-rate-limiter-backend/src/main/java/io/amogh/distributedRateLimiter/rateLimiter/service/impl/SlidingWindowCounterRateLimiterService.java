package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("sliding-window-counter")
@RequiredArgsConstructor
public class SlidingWindowCounterRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId) >= 0;
    }

    @Override
    public int getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sw-counter:" + userId;
        long now = Instant.now().getEpochSecond();
        int windowSize = properties.getWindow();
        int limit = properties.getLimit();

        long currentWindowStart = (now / windowSize) * windowSize;
        long previousWindowStart = currentWindowStart - windowSize;

        String currentWindowKey = userKey + ":" + currentWindowStart;
        String previousWindowKey = userKey + ":" + previousWindowStart;

        String currentVal = redisTemplate.opsForValue().get(currentWindowKey);
        long currentCount = Objects.nonNull(currentVal) ? Long.parseLong(currentVal) : 0;

        String prevVal = redisTemplate.opsForValue().get(previousWindowKey);
        long previousCount = Objects.nonNull(prevVal) ? Long.parseLong(prevVal) : 0;

        long previousWindowTimeRemaining = currentWindowStart + windowSize - now;
        long count = currentCount + (previousCount * previousWindowTimeRemaining) / windowSize;

        return (int) Math.max(0, limit - count);
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sw-counter:" + userId;
        long now = Instant.now().getEpochSecond();
        int windowSize = properties.getWindow();
        int limit = properties.getLimit();

        long currentWindowStart = (now / windowSize) * windowSize;
        long previousWindowStart = currentWindowStart - windowSize;

        String currentWindowKey = userKey + ":" + currentWindowStart;
        String previousWindowKey = userKey + ":" + previousWindowStart;

        String currentVal = redisTemplate.opsForValue().get(currentWindowKey);
        long currentCount = Objects.nonNull(currentVal) ? Long.parseLong(currentVal) : 0;

        String prevVal = redisTemplate.opsForValue().get(previousWindowKey);
        long previousCount = Objects.nonNull(prevVal) ? Long.parseLong(prevVal) : 0;

        redisTemplate.expire(currentWindowKey, Duration.ofSeconds(windowSize * 2));

        long previousWindowTimeRemaining = currentWindowStart + windowSize - now;
        long count = currentCount + (previousCount * previousWindowTimeRemaining) / windowSize;

        if (count >= limit) {
            return -1;
        }

        redisTemplate.opsForValue().increment(currentWindowKey);

        return (int) (limit - count - 1);
    }

}

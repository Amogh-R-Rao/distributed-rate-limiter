package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("fixed-window")
@RequiredArgsConstructor
public class FixedWindowRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId) >= 0;
    }

    @Override
    public int getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "fixed-window:" + userId;

        String currentVal = redisTemplate.opsForValue().get(userKey);
        long count = Objects.nonNull(currentVal) ? Long.parseLong(currentVal) : 0;

        return (int) Math.max(0, properties.getLimit() - count);
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "fixed-window:" + userId;
        long count = redisTemplate.opsForValue().increment(userKey);
        Duration duration = Duration.ofSeconds(properties.getWindow());

        if (count == 1) {
            redisTemplate.opsForValue().getAndExpire(userKey, duration);
        }

        if (count > properties.getLimit()) {
            redisTemplate.opsForValue().decrement(userKey);
            return -1;
        }

        return (int) (properties.getLimit() - count);
    }

}

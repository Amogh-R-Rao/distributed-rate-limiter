package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.FixedWindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("fixed-window")
@RequiredArgsConstructor
public class FixedWindowRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final FixedWindowRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        long count = redisTemplate.opsForValue().increment(userId);
        Duration duration = Duration.ofSeconds(properties.getWindow());

        if(count == 1) {
            redisTemplate.opsForValue().getAndExpire(userId, duration);
        }

        if(count > properties.getLimit()) {
            return false;
        }

        return true;
    }

    @Override
    public int getRemaining(String userId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRemaining'");
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'tryConsumeAndGetRemaining'");
    }
    
}

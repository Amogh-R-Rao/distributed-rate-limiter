package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.github.f4b6a3.uuid.UuidCreator;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("sliding-window-log")
@RequiredArgsConstructor
public class SlidingWindowLogRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId) >= 0;
    }

    @Override
    public int getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sliding-window:" + userId;
        long now = Instant.now().getEpochSecond();
        double window = now - properties.getWindow();

        redisTemplate.opsForZSet().removeRangeByScore(userKey, 0, window);

        Long currentCount = redisTemplate.opsForZSet().size(userKey);
        long actualCount = Objects.nonNull(currentCount) ? currentCount : 0;

        return (int) Math.max(0, properties.getLimit() - actualCount);
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sliding-window:" + userId;
        long now = Instant.now().getEpochSecond();
        double window = now - properties.getWindow();

        redisTemplate.opsForZSet().removeRangeByScore(userKey, 0, window);

        Long currentCount = redisTemplate.opsForZSet().size(userKey);
        long actualCount = Objects.nonNull(currentCount) ? currentCount : 0;

        if (actualCount >= properties.getLimit()) {
            return -1;
        }

        UUID requestId = UuidCreator.getTimeOrderedEpoch();
        redisTemplate.opsForZSet().add(userKey, requestId.toString(), now);
        Duration duration = Duration.ofSeconds(properties.getWindow());
        redisTemplate.expire(userKey, duration);

        return (int) (properties.getLimit() - actualCount - 1);
    }

}

package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.BucketRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("leaky-bucket")
@RequiredArgsConstructor
public class LeakyBucketRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final BucketRateLimiterProperties properties;

    @Override
    public boolean tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId) >= 0;
    }

    @Override
    public int getRemaining(String userId) {
        String queueKey = REDIS_KEY_PREFIX + "lb:queue:" + userId;
        long capacity = properties.getMaxTokens();
        long interval = properties.getRefillRate();

        long now = Instant.now().getEpochSecond();

        String oldestTsVal = redisTemplate.opsForList().index(queueKey, 0);
        if (Objects.nonNull(oldestTsVal)) {
            long oldestTs = Long.parseLong(oldestTsVal);
            long elapsed = now - oldestTs;
            long shouldLeak = elapsed / interval;

            if (shouldLeak > 0) {
                redisTemplate.opsForList().leftPop(queueKey, shouldLeak);
            }
        }

        Long queueSize = redisTemplate.opsForList().size(queueKey);
        long actualSize = Objects.nonNull(queueSize) ? queueSize : 0;

        return (int) Math.max(0, capacity - actualSize);
    }

    @Override
    public int tryConsumeAndGetRemaining(String userId) {
        String queueKey = REDIS_KEY_PREFIX + "lb:queue:" + userId;
        long capacity = properties.getMaxTokens();
        long interval = properties.getRefillRate();

        long now = Instant.now().getEpochSecond();

        String oldestTsVal = redisTemplate.opsForList().index(queueKey, 0);
        if (Objects.nonNull(oldestTsVal)) {
            long oldestTs = Long.parseLong(oldestTsVal);
            long elapsed = now - oldestTs;
            long shouldLeak = elapsed / interval;

            if (shouldLeak > 0) {
                redisTemplate.opsForList().leftPop(queueKey, shouldLeak);
            }
        }

        Long queueSize = redisTemplate.opsForList().size(queueKey);
        long actualSize = Objects.nonNull(queueSize) ? queueSize : 0;

        if (actualSize >= capacity) {
            return -1;
        }

        redisTemplate.opsForList().rightPush(queueKey, String.valueOf(now));

        Duration ttl = Duration.ofSeconds(interval).multipliedBy(capacity).multipliedBy(2);
        redisTemplate.expire(queueKey, ttl);

        return (int) (capacity - actualSize - 1);
    }

}

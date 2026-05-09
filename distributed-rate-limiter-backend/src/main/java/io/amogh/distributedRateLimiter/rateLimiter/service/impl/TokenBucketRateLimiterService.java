package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.BucketRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("token-bucket")
@RequiredArgsConstructor
public class TokenBucketRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final BucketRateLimiterProperties properties;

    private static final String LUA_SCRIPT = """
        local tokens = tonumber(redis.call('GET', KEYS[1]) or ARGV[2])
        local lastRefill = tonumber(redis.call('GET', KEYS[2]) or ARGV[1])
        local elapsed = ARGV[1] - lastRefill
        if elapsed < 0 then elapsed = 0 end
        local toAdd = (ARGV[3] * elapsed) / ARGV[4]
        tokens = math.min(ARGV[2], tokens + toAdd)
        if tokens >= 1 then
            tokens = tokens - 1
            redis.call('SET', KEYS[1], tokens, 'EX', ARGV[5])
            redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[5])
            return math.floor(tokens)
        end
        return -1
        """;

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

        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        Long result = redisTemplate.execute(
                script,
                List.of(userTokenKey, userRefillKey),
                String.valueOf(now),
                String.valueOf(capacity),
                String.valueOf(refillRate),
                String.valueOf(refillInterval),
                String.valueOf(ttl)
        );

        return result != null ? result.intValue() : -1;
    }

}

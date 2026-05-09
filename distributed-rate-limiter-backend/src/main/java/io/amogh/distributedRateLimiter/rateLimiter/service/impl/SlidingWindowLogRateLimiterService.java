package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("sliding-window-log")
@RequiredArgsConstructor
public class SlidingWindowLogRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;

    private static final String LUA_SCRIPT = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[2])
        local count = redis.call('ZCARD', KEYS[1])
        if count >= tonumber(ARGV[3]) then
            return -1
        end
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[5])
        redis.call('EXPIRE', KEYS[1], ARGV[4])
        return tonumber(ARGV[3]) - count - 1
        """;

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
        double windowStart = now - properties.getWindow();
        Duration duration = Duration.ofSeconds(properties.getWindow());
        String requestId = UUID.randomUUID().toString();

        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        Long result = redisTemplate.execute(
                script,
                List.of(userKey),
                String.valueOf(now),
                String.valueOf(windowStart),
                String.valueOf(properties.getLimit()),
                String.valueOf(duration.getSeconds()),
                requestId
        );

        return result != null ? result.intValue() : -1;
    }

}

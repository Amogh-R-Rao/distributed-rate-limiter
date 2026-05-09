package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("fixed-window")
@RequiredArgsConstructor
public class FixedWindowRateLimiterService implements IRateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;

    private static final String LUA_SCRIPT = """
        local count = redis.call('INCR', KEYS[1])
        if count == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[2])
        end
        if count > tonumber(ARGV[1]) then
            return -1
        end
        return tonumber(ARGV[1]) - count
        """;

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
        Duration duration = Duration.ofSeconds(properties.getWindow());

        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        Long result = redisTemplate.execute(
                script,
                List.of(userKey),
                String.valueOf(properties.getLimit()),
                String.valueOf(duration.getSeconds())
        );

        return result != null ? result.intValue() : -1;
    }

}

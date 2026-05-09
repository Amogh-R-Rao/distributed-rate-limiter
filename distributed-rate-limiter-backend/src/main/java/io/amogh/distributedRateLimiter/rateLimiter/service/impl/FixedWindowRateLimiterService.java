package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.util.List;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service("fixed-window")
@RequiredArgsConstructor
public class FixedWindowRateLimiterService implements IRateLimiterService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
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
    public Mono<Boolean> tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId).map(result -> result >= 0);
    }

    @Override
    public Mono<Integer> getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "fixed-window:" + userId;

        return redisTemplate.opsForValue().get(userKey)
                .map(val -> {
                    long count = Long.parseLong(val);
                    return (int) Math.max(0, properties.getLimit() - count);
                })
                .defaultIfEmpty(properties.getLimit());
    }

    @Override
    public Mono<Integer> tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "fixed-window:" + userId;
        Duration duration = Duration.ofSeconds(properties.getWindow());

        RedisScript<Long> script = RedisScript.of(LUA_SCRIPT, Long.class);

        return redisTemplate.execute(
                        script,
                        List.of(userKey),
                        String.valueOf(properties.getLimit()),
                        String.valueOf(duration.getSeconds())
                )
                .next()
                .map(Long::intValue)
                .defaultIfEmpty(-1);
    }

}

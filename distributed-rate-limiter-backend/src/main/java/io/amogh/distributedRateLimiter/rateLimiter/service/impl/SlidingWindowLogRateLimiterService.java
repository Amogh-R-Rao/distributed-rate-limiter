package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service("sliding-window-log")
@RequiredArgsConstructor
public class SlidingWindowLogRateLimiterService implements IRateLimiterService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
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
    public Mono<Boolean> tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId).map(result -> result >= 0);
    }

    @Override
    public Mono<Integer> getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sliding-window:" + userId;
        long now = Instant.now().getEpochSecond();
        double window = now - properties.getWindow();

        return redisTemplate.opsForZSet().removeRangeByScore(userKey, Range.closed(0.0, window))
                .flatMap(removed -> redisTemplate.opsForZSet().size(userKey))
                .switchIfEmpty(redisTemplate.opsForZSet().size(userKey))
                .map(count -> (int) Math.max(0, properties.getLimit() - count))
                .defaultIfEmpty(properties.getLimit());
    }

    @Override
    public Mono<Integer> tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sliding-window:" + userId;
        long now = Instant.now().getEpochSecond();
        double windowStart = now - properties.getWindow();
        Duration duration = Duration.ofSeconds(properties.getWindow());
        String requestId = UUID.randomUUID().toString();

        RedisScript<Long> script = RedisScript.of(LUA_SCRIPT, Long.class);

        return redisTemplate.execute(
                        script,
                        List.of(userKey),
                        String.valueOf(now),
                        String.valueOf(windowStart),
                        String.valueOf(properties.getLimit()),
                        String.valueOf(duration.getSeconds()),
                        requestId
                )
                .next()
                .map(Long::intValue)
                .defaultIfEmpty(-1);
    }

}

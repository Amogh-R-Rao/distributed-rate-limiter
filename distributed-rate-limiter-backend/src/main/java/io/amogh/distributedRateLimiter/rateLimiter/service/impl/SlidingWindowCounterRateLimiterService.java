package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service("sliding-window-counter")
@RequiredArgsConstructor
public class SlidingWindowCounterRateLimiterService implements IRateLimiterService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final WindowRateLimiterProperties properties;

    private static final String LUA_SCRIPT = """
        local currentCount = tonumber(redis.call('GET', KEYS[1]) or '0')
        local previousCount = tonumber(redis.call('GET', KEYS[2]) or '0')
        redis.call('EXPIRE', KEYS[1], ARGV[4])
        local elapsedInWindow = ARGV[1] % ARGV[2]
        local weightedPrev = (previousCount * (ARGV[2] - elapsedInWindow)) / ARGV[2]
        local count = currentCount + weightedPrev
        if count >= tonumber(ARGV[3]) then
            return -1
        end
        redis.call('INCR', KEYS[1])
        return tonumber(ARGV[3]) - math.floor(count) - 1
        """;

    @Override
    public Mono<Boolean> tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId).map(result -> result >= 0);
    }

    @Override
    public Mono<Integer> getRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sw-counter:" + userId;
        long now = Instant.now().getEpochSecond();
        int windowSize = properties.getWindow();
        int limit = properties.getLimit();

        long currentWindowStart = (now / windowSize) * windowSize;
        long previousWindowStart = currentWindowStart - windowSize;

        String currentWindowKey = userKey + ":" + currentWindowStart;
        String previousWindowKey = userKey + ":" + previousWindowStart;

        Mono<String> currentMono = redisTemplate.opsForValue().get(currentWindowKey);
        Mono<String> previousMono = redisTemplate.opsForValue().get(previousWindowKey);

        return Mono.zip(currentMono, previousMono)
                .map(tuple -> {
                    String currentVal = tuple.getT1();
                    String prevVal = tuple.getT2();

                    long currentCount = currentVal != null ? Long.parseLong(currentVal) : 0;
                    long previousCount = prevVal != null ? Long.parseLong(prevVal) : 0;

                    long previousWindowTimeRemaining = currentWindowStart + windowSize - now;
                    long count = currentCount + (previousCount * previousWindowTimeRemaining) / windowSize;

                    return (int) Math.max(0, limit - count);
                })
                .switchIfEmpty(Mono.just(limit));
    }

    @Override
    public Mono<Integer> tryConsumeAndGetRemaining(String userId) {
        String userKey = REDIS_KEY_PREFIX + "sw-counter:" + userId;
        long now = Instant.now().getEpochSecond();
        int windowSize = properties.getWindow();
        int limit = properties.getLimit();

        long currentWindowStart = (now / windowSize) * windowSize;
        long previousWindowStart = currentWindowStart - windowSize;

        String currentWindowKey = userKey + ":" + currentWindowStart;
        String previousWindowKey = userKey + ":" + previousWindowStart;

        RedisScript<Long> script = RedisScript.of(LUA_SCRIPT, Long.class);

        return redisTemplate.execute(
                        script,
                        List.of(currentWindowKey, previousWindowKey),
                        String.valueOf(now),
                        String.valueOf(windowSize),
                        String.valueOf(limit),
                        String.valueOf(windowSize * 2)
                )
                .next()
                .map(Long::intValue)
                .defaultIfEmpty(-1);
    }

}

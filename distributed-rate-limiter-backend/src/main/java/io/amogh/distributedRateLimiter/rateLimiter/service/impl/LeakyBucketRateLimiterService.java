package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import io.amogh.distributedRateLimiter.rateLimiter.property.BucketRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service("leaky-bucket")
@RequiredArgsConstructor
public class LeakyBucketRateLimiterService implements IRateLimiterService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final BucketRateLimiterProperties properties;

    private static final String LUA_SCRIPT = """
        local oldest = redis.call('LINDEX', KEYS[1], 0)
        if oldest then
            local elapsed = ARGV[1] - tonumber(oldest)
            local shouldLeak = math.floor(elapsed / ARGV[3])
            if shouldLeak > 0 then
                redis.call('LTRIM', KEYS[1], shouldLeak, -1)
            end
        end
        local size = redis.call('LLEN', KEYS[1])
        if size >= tonumber(ARGV[2]) then
            return -1
        end
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[4])
        return tonumber(ARGV[2]) - size - 1
        """;

    @Override
    public Mono<Boolean> tryConsume(String userId) {
        return tryConsumeAndGetRemaining(userId).map(result -> result >= 0);
    }

    @Override
    public Mono<Integer> getRemaining(String userId) {
        String queueKey = REDIS_KEY_PREFIX + "lb:queue:" + userId;
        long capacity = properties.getMaxTokens();
        long interval = properties.getRefillRate();
        long now = Instant.now().getEpochSecond();

        return redisTemplate.opsForList().index(queueKey, 0)
                .flatMap(oldestTsVal -> {
                    long oldestTs = Long.parseLong(oldestTsVal);
                    long elapsed = now - oldestTs;
                    long shouldLeak = elapsed / interval;

                    if (shouldLeak > 0) {
                        return redisTemplate.opsForList().leftPop(queueKey, shouldLeak)
                                .then(redisTemplate.opsForList().size(queueKey));
                    }
                    return redisTemplate.opsForList().size(queueKey);
                })
                .switchIfEmpty(redisTemplate.opsForList().size(queueKey))
                .map(size -> (int) Math.max(0, capacity - size))
                .defaultIfEmpty((int) capacity);
    }

    @Override
    public Mono<Integer> tryConsumeAndGetRemaining(String userId) {
        String queueKey = REDIS_KEY_PREFIX + "lb:queue:" + userId;
        long capacity = properties.getMaxTokens();
        long interval = properties.getRefillRate();
        long now = Instant.now().getEpochSecond();
        Duration ttl = Duration.ofSeconds(interval).multipliedBy(capacity).multipliedBy(2);

        RedisScript<Long> script = RedisScript.of(LUA_SCRIPT, Long.class);

        return redisTemplate.execute(
                        script,
                        List.of(queueKey),
                        String.valueOf(now),
                        String.valueOf(capacity),
                        String.valueOf(interval),
                        String.valueOf(ttl.getSeconds())
                )
                .next()
                .map(Long::intValue)
                .defaultIfEmpty(-1);
    }

}

package io.amogh.distributedRateLimiter.rateLimiter.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.github.f4b6a3.uuid.UuidCreator;

import io.amogh.distributedRateLimiter.rateLimiter.property.WindowRateLimiterProperties;
import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import lombok.RequiredArgsConstructor;

@Service("sliding-window")
@RequiredArgsConstructor
public class SlidingWindowRateLimiterService  implements IRateLimiterService {
    
    private final StringRedisTemplate redisTemplate;
    private final WindowRateLimiterProperties properties;


    @Override
    public boolean tryConsume(String userId) {

        String userKey = REDIS_KEY_PREFIX + "sliding-window:" + userId;
        long now = Instant.now().getEpochSecond();
        double window = now - properties.getWindow();
        redisTemplate.opsForZSet().removeRangeByScore(userKey, 0, window);
        long currentCount = redisTemplate.opsForZSet().size(userKey);

        if(currentCount >= properties.getLimit()) {
            return false;
        }

        UUID requestId = UuidCreator.getTimeOrderedEpoch();
        redisTemplate.opsForZSet().add(userKey, requestId.toString(), now);
        Duration duration = Duration.ofSeconds(properties.getWindow());

        redisTemplate.expire(userKey, duration);

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

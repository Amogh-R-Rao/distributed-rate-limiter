package io.amogh.distributedRateLimiter.rateLimiter.property;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@ConfigurationProperties(prefix = "rate-limiter.token-bucket")
@Data
public class BucketRateLimiterProperties {
    
    private long refillRate;
    private long refillAmount;
    private long maxTokens;

}

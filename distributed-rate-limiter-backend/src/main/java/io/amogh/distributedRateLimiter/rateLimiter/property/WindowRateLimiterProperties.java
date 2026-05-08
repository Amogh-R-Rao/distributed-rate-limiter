package io.amogh.distributedRateLimiter.rateLimiter.property;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@ConfigurationProperties(prefix = "rate-limiter.window")
@Data
public class WindowRateLimiterProperties {
    
    private int limit;
    private int window;

}

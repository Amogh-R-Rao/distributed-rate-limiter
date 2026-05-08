package io.amogh.distributedRateLimiter.rateLimiter.service;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
public class RateLimiterServiceFactory {

    private final Map<String, IRateLimiterService> services;

    public RateLimiterServiceFactory(Map<String, IRateLimiterService> services) {
        this.services = services;
    }

    public IRateLimiterService getService(String type) {
        IRateLimiterService  service = services.get(type);
        if(Objects.isNull(service)) {
            throw new IllegalArgumentException("Unsupported Service Type");
        }
        return service;
    }

}

package io.amogh.distributedRateLimiter.rateLimiter.service;

import java.util.Map;

import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;

@Component
public class RateLimiterServiceFactory {

    private final Map<String, IRateLimiterService> services;

    public RateLimiterServiceFactory(Map<String, IRateLimiterService> services) {
        this.services = services;
    }

    public Mono<IRateLimiterService> getService(String type) {
        IRateLimiterService service = services.get(type);
        if (service == null) {
            return Mono.error(new IllegalArgumentException("Unsupported Service Type"));
        }
        return Mono.just(service);
    }

}

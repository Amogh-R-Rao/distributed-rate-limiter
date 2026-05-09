package io.amogh.distributedRateLimiter.rateLimiter.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.amogh.distributedRateLimiter.rateLimiter.service.RateLimiterServiceFactory;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/rate-limit")
@RequiredArgsConstructor
public class RateLimitController {

    private final RateLimiterServiceFactory rateLimiterServiceFactory;

    @PostMapping("/{limiter}/user/{userId}")
    public Mono<Boolean> tryConsume(@PathVariable String limiter, @PathVariable String userId) {
        return rateLimiterServiceFactory.getService(limiter)
                .flatMap(service -> service.tryConsume(userId));
    }

}

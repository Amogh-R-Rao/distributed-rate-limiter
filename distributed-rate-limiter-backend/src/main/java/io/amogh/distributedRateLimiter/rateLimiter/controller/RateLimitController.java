package io.amogh.distributedRateLimiter.rateLimiter.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.amogh.distributedRateLimiter.rateLimiter.service.IRateLimiterService;
import io.amogh.distributedRateLimiter.rateLimiter.service.RateLimiterServiceFactory;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/rate-limit")
@RequiredArgsConstructor
public class RateLimitController {
    
    private final RateLimiterServiceFactory rateLimiterServiceFactory;

    @PostMapping("/{limiter}/user/{userId}")
    public boolean tryConsume(@PathVariable String limiter, @PathVariable String userId) {
        IRateLimiterService rateLimiterService = rateLimiterServiceFactory.getService(limiter);
        return rateLimiterService.tryConsume(userId);
    }

}

package io.amogh.distributedRateLimiter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DistributedRateLimiterBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(DistributedRateLimiterBackendApplication.class, args);
	}

}

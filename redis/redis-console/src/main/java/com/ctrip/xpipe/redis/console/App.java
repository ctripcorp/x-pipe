package com.ctrip.xpipe.redis.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ctrip.xpipe.redis.console.health.HealthChecker;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@SpringBootApplication
public class App {
	public static void main(String[] args){
		System.setProperty("spring.profiles.active", "production");
		System.setProperty(HealthChecker.ENABLED, "false");
		SpringApplication.run(App.class, args);
	}
}

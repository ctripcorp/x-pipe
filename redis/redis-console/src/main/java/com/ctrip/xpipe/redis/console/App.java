package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@SpringBootApplication
@ComponentScan(excludeFilters = { @ComponentScan.Filter(type = FilterType.REGEX,pattern = "com.ctrip.xpipe.redis.console.((?!spring).)*") })
public class App {
	public static void main(String[] args){
		System.setProperty("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
		System.setProperty(HealthChecker.ENABLED, "true");
		System.setProperty("console.do.checker", "true");
		System.setProperty("console.do.manager", "false");
		SpringApplication.run(App.class, args);
	}
}

package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@ComponentScan("com.ctrip.xpipe.redis.console.spring")
public class App {
	public static void main(String[] args){
		System.setProperty("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
		System.setProperty(HealthChecker.ENABLED, "true");
		System.setProperty("DisableLoadProxyAgentJar", "true");
		SpringApplication.run(App.class, args);
	}
}

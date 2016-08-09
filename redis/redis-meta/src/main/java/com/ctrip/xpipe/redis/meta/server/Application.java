package com.ctrip.xpipe.redis.meta.server;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@EnableAutoConfiguration
@Import(com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.class)
public class Application {
	
	public static void main(String[] args) {
		
		System.setProperty("spring.profiles.active", "production");
		SpringApplication springApplication = new SpringApplication(Application.class);
		springApplication.run(args);
	}
}

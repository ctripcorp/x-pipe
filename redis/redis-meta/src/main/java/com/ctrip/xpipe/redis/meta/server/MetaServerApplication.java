package com.ctrip.xpipe.redis.meta.server;



import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@SpringBootApplication
public class MetaServerApplication {
	
	public static void main(String[] args) {
		
		System.setProperty("spring.profiles.active", "production");
		SpringApplication springApplication = new SpringApplication(MetaServerApplication.class);
		
		springApplication.run(args);
		
	}
}

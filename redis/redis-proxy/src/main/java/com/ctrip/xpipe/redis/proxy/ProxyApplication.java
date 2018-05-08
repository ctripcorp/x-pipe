package com.ctrip.xpipe.redis.proxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
@SpringBootApplication
public class ProxyApplication {

    public static void main(String[] args) {

        System.setProperty("spring.profiles.active", "production");
        SpringApplication springApplication = new SpringApplication(ProxyApplication.class);

        springApplication.run(args);
    }
}

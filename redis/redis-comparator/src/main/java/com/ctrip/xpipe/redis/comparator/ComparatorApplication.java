package com.ctrip.xpipe.redis.comparator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * redis-comparator 入口。profile 与 {@code ProxyApplication} 逐行对齐（spec §4.6.1b）。
 */
@SpringBootApplication
public class ComparatorApplication {

    public static void main(String[] args) throws Exception {

        System.setProperty("spring.profiles.active", "production");
        SpringApplication application = new SpringApplication(ComparatorApplication.class);

        application.run(args);

    }
}

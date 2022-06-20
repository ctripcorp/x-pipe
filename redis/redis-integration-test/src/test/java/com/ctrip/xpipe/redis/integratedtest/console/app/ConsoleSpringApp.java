package com.ctrip.xpipe.redis.integratedtest.console.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(
        value = {"com.ctrip.xpipe.redis.integratedtest.console.spring.console", "com.ctrip.xpipe.redis.console.spring"}
        , excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = {
                com.ctrip.xpipe.redis.console.spring.CheckerContextConfig.class,
                com.ctrip.xpipe.redis.console.spring.ConsoleCheckerContextConfig.class,
                com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig.class
        })
)
public class ConsoleSpringApp {

    public static void main(String[] args) {
        SpringApplication.run(ConsoleSpringApp.class, args);
    }

}
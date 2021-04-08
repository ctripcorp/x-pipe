package com.ctrip.xpipe.redis.integratedtest.console.app;

import com.ctrip.xpipe.redis.console.App;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author lishanglin
 * date 2021/1/21
 */
@SpringBootApplication
public class ConsoleApp {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

}
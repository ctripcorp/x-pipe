package com.ctrip.xpipe.redis.integratedtest.console.app;

import com.ctrip.xpipe.redis.meta.server.MetaServerApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author lishanglin
 * date 2021/1/22
 */
@SpringBootApplication
public class MetaserverApp {

    public static void main(String[] args) {
        SpringApplication.run(MetaServerApplication.class, args);
    }

}

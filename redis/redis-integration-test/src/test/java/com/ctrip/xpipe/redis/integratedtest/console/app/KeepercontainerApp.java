package com.ctrip.xpipe.redis.integratedtest.console.app;

import com.ctrip.xpipe.redis.keeper.KeeperContainerApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author lishanglin
 * date 2021/1/22
 */
@SpringBootApplication
public class KeepercontainerApp {

    public static void main(String[] args) {
        SpringApplication.run(KeeperContainerApplication.class, args);
    }

}

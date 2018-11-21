package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.proxy.controller.ComponentRegistryHolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
@SpringBootApplication
public class ProxyApplication {

    public static void main(String[] args) throws Exception {

        System.setProperty("spring.profiles.active", "production");
        SpringApplication application = new SpringApplication(ProxyApplication.class);

        application.run(args);

    }

    private static void initComponentRegistry(ConfigurableApplicationContext context) throws Exception {

        final ComponentRegistry registry = new SpringComponentRegistry(context);
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }
}

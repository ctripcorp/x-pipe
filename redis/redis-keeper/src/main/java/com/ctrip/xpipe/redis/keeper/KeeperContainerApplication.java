package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@SpringBootApplication
public class KeeperContainerApplication {
    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(KeeperContainerApplication.class).run(args);
        initComponentRegistry(context);
    }

    private static void initComponentRegistry(ApplicationContext context) throws Exception {
        ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }
}

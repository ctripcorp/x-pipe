package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.keeper.spring.TestWithoutZkProfile;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.impl.TestZkClient;

import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@SpringBootApplication
public class AppTest extends AbstractRedisKeeperTest {
	
    @Test
    public void start8080() throws Exception {
        System.setProperty("server.port", "8080");
        System.setProperty("replication.store.dir", "/opt/data/xpipe8080");
        start();
    }

    @Test
    public void start8081() throws Exception {
        System.setProperty("server.port", "8081");
        System.setProperty("replication.store.dir", "/opt/data/xpipe8081");
        start();
    }

    @Test
    public void start8180() throws Exception {
        System.setProperty("server.port", "8180");
        System.setProperty(TestZkClient.ZK_ADDRESS_KEY, "127.0.0.1:2182");
        System.setProperty("replication.store.dir", "/opt/data/xpipe8180");
        start();
    }

    @Test
    public void start8181() throws Exception {
        System.setProperty("server.port", "8181");
        System.setProperty(TestZkClient.ZK_ADDRESS_KEY, "127.0.0.1:2182");
        System.setProperty("replication.store.dir", "/opt/data/xpipe8181");
        start();
    }


    private void start() throws Exception {
    	System.setProperty(AbstractProfile.PROFILE_KEY, TestWithoutZkProfile.PROFILE_NO_ZK);
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(AppTest.class).run();
        initComponentRegistry(context);
        waitForAnyKeyToExit();
    }

    private void initComponentRegistry(ApplicationContext context) throws Exception {
        ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }
}

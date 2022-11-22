package com.ctrip.xpipe.redis.keeper.index;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.AppTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Slight
 * <p>
 * Nov 08, 2022 13:32
 */
public class Gtid2OffsetIndexGeneratorTest extends AbstractRedisKeeperContextTest {

    @Test
    public void manual() throws Exception {
        RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
        redisKeeperServer.initialize();
        redisKeeperServer.start();

        redisKeeperServer.getReplicationStore().getMetaStore().becomeActive();
        redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", 6379)));
        redisKeeperServer.reconnectMaster();

        //redisKeeperServer.startIndexing();

        sleep(5000);

        RedisMeta redis = new RedisMeta();
        redis.setIp("127.0.0.1");
        redis.setPort(6379);

        sendRandomMessage(redis, 30);

        waitForAnyKey();
    }

    private void setReplicationStoreDir() {
        System.setProperty("replication.store.dir", String.format("/opt/data/xpipe%s", System.getProperty("server.port")));
    }

    private void start() throws Exception {
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(AppTest.class).run();
        initComponentRegistry(context);
        waitForAnyKeyToExit();
    }

    private void initComponentRegistry(ConfigurableApplicationContext  context) throws Exception {
        ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "keeper-test.xml";
    }
}
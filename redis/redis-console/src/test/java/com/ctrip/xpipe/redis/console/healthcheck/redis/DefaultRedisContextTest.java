package com.ctrip.xpipe.redis.console.healthcheck.redis;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckRedisInstanceFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultRedisContextTest extends DefaultHealthCheckRedisInstanceFactoryTest {

    private DefaultRedisContext context;

    @Before
    public void beforeDefaultRedisContextTest() {
        DcMeta dcMeta = new DcMeta().setId("dc");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster_shyin").setParent(dcMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard2");
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("10.2.58.244").setPort(6389);
        RedisHealthCheckInstance instance = factory.create(redisMeta);
        context = (DefaultRedisContext) instance.getHealthCheckContext().getRedisContext();
    }

    @Test
    public void testDoScheduledTask() throws InterruptedException {
        context.doScheduledTask();
        Thread.sleep(1000);
        logger.info("[role] {}", context.getRole());
        logger.info("[offset] {}", context.getReplOffset());
        logger.info("[isMaster] {}", context.isMater());
    }

    @Test
    public void testDoInitialize() {
    }
}
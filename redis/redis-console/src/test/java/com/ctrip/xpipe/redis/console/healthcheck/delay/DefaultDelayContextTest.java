package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckRedisInstanceFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.redis.DefaultRedisContext;
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
public class DefaultDelayContextTest extends DefaultHealthCheckRedisInstanceFactoryTest {

    private DefaultDelayContext context;

    @Before
    public void beforeDefaultRedisContextTest() throws Exception {
        DcMeta dcMeta = new DcMeta().setId("dc");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster_shyin").setParent(dcMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard2");
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("10.2.58.244").setPort(6389);
        RedisHealthCheckInstance instance = factory.create(redisMeta);
        instance.getHealthCheckContext().getRedisContext().initialize();
        context = (DefaultDelayContext) instance.getHealthCheckContext().getDelayContext();

    }

    @Test
    public void testDoScheduledTask() throws InterruptedException {
        Thread.sleep(1000 * 2);
        context.doScheduledTask();
        Thread.sleep(1000 * 2);
        logger.info("[last-delay-nano]{}", context.lastDelayNano());
        logger.info("[last-pub-nano]{}", context.lastDelayPubTimeNano());
        logger.info("[last-delay-time]{}", context.lastTimeDelayMilli());
    }
}
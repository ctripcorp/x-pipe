package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthCheckContextFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultHealthCheckContextFactory factory;

    @Test
    public void testCreate() {
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster");
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        HealthCheckContext context = factory.create(new DefaultRedisHealthCheckInstance(), redisMeta);
        Assert.assertNotNull(context.getDelayContext());
        Assert.assertNotNull(context.getPingContext());
        Assert.assertNotNull(context.getRedisConfContext());
        Assert.assertNotNull(context.getRedisContext());
        Assert.assertTrue(context.getLifecycleState().canStart());
    }
}
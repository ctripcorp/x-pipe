package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.factory.RedisHealthCheckInstanceFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 17, 2018
 */
public class DefaultRedisHealthCheckInstanceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RedisHealthCheckInstanceFactory factory;

    @Test
    public void testDoStop() throws Exception {
        RedisHealthCheckInstance instance = factory.create(newRandomFakeRedisMeta());
        instance.stop();
    }
}
package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2018
 */
public class DefaultInstanceUpHandlerTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private DefaultInstanceUpHandler handler;


    @Test
    public void testDoHandle() {
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo("FAT",
                "cluster_shyin", "shard1", new HostPort("10.3.2.220", 6379), "FAT", ClusterType.ONE_WAY);
        when(instance.getCheckInfo()).thenReturn(info);
        handler.doHandle(new InstanceUp(instance));
        sleep(10 * 1000);
    }
}
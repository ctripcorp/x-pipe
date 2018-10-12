package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2018
 */
public class DefaultRedisHealthCheckInstanceFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultRedisHealthCheckInstanceFactory factory;

    @Test
    public void create() throws Exception {
        Server server = startServer("+PONG\r\n");
        CrossDcClusterServer clusterServer = mock(CrossDcClusterServer.class);
        when(clusterServer.amILeader()).thenReturn(true);
        factory.setClusterServer(clusterServer);
        RedisHealthCheckInstance instance = factory.create(newRandomFakeRedisMeta().setPort(server.getPort()));
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            logger.info("[action] {}", action);
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                logger.info("   [listener] {}", listener);
            }
        }
    }
}
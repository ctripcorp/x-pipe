package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServer;
import com.ctrip.xpipe.redis.console.controller.api.HealthController;
import com.ctrip.xpipe.redis.console.healthcheck.*;
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

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private ConsoleCrossDcServer crossDcServer;

    @Autowired
    private HealthController controller;

    @Test
    public void create() throws Exception {
        Server server = startServer("+PONG\r\n");
        crossDcServer.start();
        factory.setClusterServer(crossDcServer);
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta().setPort(server.getPort()));
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            logger.info("[action] {}", action);
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                logger.info("   [listener] {}", listener);
            }
        }
        logger.info("=====================splitter=======================================");
        crossDcServer.setCrossDcLeader(true, "leader");
        sleep(100);
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            logger.info("[action] {}", action);
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                logger.info("   [listener] {}", listener);
            }
        }

        logger.info("{}", controller.getHealthCheckInstance(instance.getEndpoint().getHost(), instance.getEndpoint().getPort()));
    }
}
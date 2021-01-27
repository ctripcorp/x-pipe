package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.controller.api.HealthController;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Mockito.mock;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2018
 */
public class DefaultHealthCheckInstanceFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultHealthCheckInstanceFactory factory;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private ConsoleLeaderElector crossDcServer;

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
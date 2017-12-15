package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.action.HealthStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Dec 15, 2017
 */
public class LogTest extends AbstractConsoleIntegrationTest {

    Logger logger = LoggerFactory.getLogger(LogTest.class);

    @Autowired
    private ConsoleConfig consoleConfig;

    @Test
    public void testLog() {
        HostPort hostPort = new HostPort("10.2.75.143", 6379);
        HealthStatus healthStatus = new HealthStatus(
                hostPort,
                () -> consoleConfig.getDownAfterCheckNums() * consoleConfig.getRedisReplicationHealthCheckInterval(),
                () -> consoleConfig.getHealthyDelayMilli(),
                scheduled);
        logger.info("[begin]");
        healthStatus.delay(System.currentTimeMillis());
        logger.info("[end]");
    }


}

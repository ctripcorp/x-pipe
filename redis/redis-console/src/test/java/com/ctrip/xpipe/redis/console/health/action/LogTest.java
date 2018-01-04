package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chen.zhu
 * <p>
 * Dec 15, 2017
 */
public class LogTest extends AbstractConsoleIntegrationTest {

    Logger logger = LoggerFactory.getLogger(LogTest.class);

    @Autowired
    private ConsoleConfig consoleConfig;

    private static final int N = 1500;

    ExecutorService executor = Executors.newFixedThreadPool(1500);

    @Test
    public void testLog() {
        List<HealthStatus> healthStatuses = generateHealthStatus();
        for(int i = 0; i < 3600 * 24; i ++) {
            for (HealthStatus healthStatus : healthStatuses) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        healthStatus.delay(System.currentTimeMillis());
                    }
                });
            }
        }
    }

    private List<HealthStatus> generateHealthStatus() {
        List<HealthStatus> result = new ArrayList<>(N);
        Random random = new Random();
        for(int i = 0; i < N; i++) {
            String ip = String.format("10.2.%d.%d", random.nextInt(255), random.nextInt(255));
            int port = random.nextInt(6535);
            result.add(createHealthStatus(ip, port));
        }
        return result;
    }

    private HealthStatus createHealthStatus(String host, int port) {
        HostPort hostPort = new HostPort("10.2.75.143", 6379);
        return new HealthStatus(
                hostPort,
                () -> consoleConfig.getDownAfterCheckNums() * consoleConfig.getRedisReplicationHealthCheckInterval(),
                () -> consoleConfig.getHealthyDelayMilli(),
                scheduled);
    }
}

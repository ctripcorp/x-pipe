package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DefaultSocketStatsManagerTest extends AbstractRedisProxyServerTest {

    private DefaultSocketStatsManager socketStatsManager = (DefaultSocketStatsManager) proxyResourceManager.getSocketStatsManager();

    private String socketStatFormat = "ESTAB      0      0         ::ffff:127.0.0.1:%d          ::ffff:127.0.0.1:%d";

    @Test
    public void testAnalyzeRawSocketStats() {
        List<String> rawSocketStats = new ArrayList<>();

        Map<DefaultSocketStatsManager.LocalRemotePort, SocketStatsResult> newSocketStats = socketStatsManager.analyzeRawSocketStats(rawSocketStats);
        Assert.assertEquals(0, newSocketStats.size());

        int[] ports = {randomPort(), randomPort(), randomPort(), randomPort(), randomPort(), randomPort(), randomPort(), randomPort()};
        for (int i = 0; i < ports.length; i += 2) {
            rawSocketStats.add(String.format(socketStatFormat, ports[i], ports[i + 1]));
            rawSocketStats.add(randomString(500));
        }
        newSocketStats = socketStatsManager.analyzeRawSocketStats(rawSocketStats);
        Assert.assertEquals(4, newSocketStats.size());
        SocketStatsResult socketStatsResult = null;
        for (int i = 0; i < ports.length - 2; i += 2) {
            socketStatsResult = newSocketStats.get(new DefaultSocketStatsManager.LocalRemotePort(ports[i], ports[i + 1]));
            Assert.assertEquals(2, socketStatsResult.getResult().size());
        }

        rawSocketStats.remove(rawSocketStats.get(rawSocketStats.size() - 1));
        newSocketStats = socketStatsManager.analyzeRawSocketStats(rawSocketStats);
        Assert.assertEquals(0, newSocketStats.size());
    }

    @Test
    @Ignore
    public void test4WConnections() {
        long startTime1 = System.currentTimeMillis();
        List<String> strings = generate4WConnections();
        long startTime2 = System.currentTimeMillis();
        logger.info("generate data spend time:{}", startTime2 - startTime1);
        socketStatsManager.analyzeRawSocketStats(strings);
        long endTime = System.currentTimeMillis();
        logger.info("deal data spend time:{}", endTime - startTime2);
    }

    private List<String> generate4WConnections() {
        List<String> result = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 40000; i++) {
            result.add(String.format(socketStatFormat, random.nextInt(100000), random.nextInt(100000)));
            result.add(randomString(500));
        }
        return result;
    }
}
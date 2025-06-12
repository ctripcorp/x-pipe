package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.RedisMsgCollector;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class RedisMsgReporterTest {

    private RedisMsgReporter redisMsgReporter;

    @Mock
    private RedisMsgCollector redisMsgCollector;

    @Mock
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private CheckerConfig config;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        redisMsgReporter = new RedisMsgReporter(redisMsgCollector, checkerConsoleService, config);
        redisMsgReporter.init();
    }

    @Test
    public void testIsLeader() {
        redisMsgReporter.isleader();
    }

    @Test
    public void testNotLeader() {
        redisMsgReporter.notLeader();
    }

    @Test
    public void testReportKeeperContainerInfo() {
        Map<String, Map<HostPort, RedisMsg>> redisMsgMap = new HashMap<>();
//        Mockito.when(redisMsgCollector.getRedisMasterMsgMap()).thenReturn(redisMsgMap);

        redisMsgReporter.reportKeeperContainerInfo();
    }

    @Test
    public void testEqualsAndHashCode() {
        RedisMsg msg1 = new RedisMsg(100, 200, 300);
        RedisMsg msg2 = new RedisMsg(100, 200, 300);
        RedisMsg msg3 = new RedisMsg(200, 300, 400);

        Assert.assertEquals(msg1, msg2);
        Assert.assertEquals(msg1.hashCode(), msg2.hashCode());

        Assert.assertNotEquals(msg1, msg3);
        Assert.assertNotEquals(msg1.hashCode(), msg3.hashCode());
    }

    @Test
    public void testToString() {
        RedisMsg msg = new RedisMsg(100, 200, 300);
        String expected = "RedisMsg{inPutFlow=100, usedMemory=200, offset=300}";
        Assert.assertEquals(expected, msg.toString());
    }

}
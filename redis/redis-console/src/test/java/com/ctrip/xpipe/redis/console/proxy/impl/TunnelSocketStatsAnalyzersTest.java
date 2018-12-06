package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.AbstractTest;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TunnelSocketStatsAnalyzersTest extends AbstractTest {

    private List<String> socketStats = Lists.newArrayList("ESTAB 142 143 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
            "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114");

    @Test
    public void testSendQueue() {
        double result = new DefaultTunnelSocketStatsAnalyzerManager.SendQueueAnalyzer().analyze(socketStats);
        logger.info("[result] {}", result);
        Assert.assertEquals(143, result, 0);
    }

    @Test
    public void testRecvQueue() {
        double result = new DefaultTunnelSocketStatsAnalyzerManager.RecvQueueAnalyzer().analyze(socketStats);
        logger.info("[result] {}", result);
        Assert.assertEquals(142, result, 0);
    }
}

package com.ctrip.xpipe.redis.console.proxy.impl;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestForAbstractMultiValueTunnelSocketStatsAnalyzer {

    private Logger logger = LoggerFactory.getLogger(TestForAbstractMultiValueTunnelSocketStatsAnalyzer.class);

    private AbstractMultiValueTunnelSocketStatsAnalyzer analyzer;

    private String key;

    @Before
    public void beforeTestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer() {
        key = "retrans";
        analyzer = new AbstractMultiValueTunnelSocketStatsAnalyzer(key) {
            @Override
            protected double getValueFromString(String[] values) {
                return parseDouble(values[1]);
            }

            @Override
            public String getType() {
                return key;
            }
        };
    }


    @Test
    public void analyzeRetrans() {
        double result = analyzer.analyze(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
                "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114"));
        Assert.assertEquals(9052, result, 0);
        logger.info("result: {}", result);
    }

    @Test
    public void analyzeRtt() {
        key = "rtt";
        analyzer = new AbstractMultiValueTunnelSocketStatsAnalyzer(key) {
            @Override
            protected double getValueFromString(String[] values) {
                return parseDouble(values[0]);
            }

            @Override
            public String getType() {
                return key;
            }
        };
        double result = analyzer.analyze(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
                "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114"));
        Assert.assertEquals(256.633, result, 1);
        logger.info("result: {}", result);
    }

    @Test
    public void testrb() {
        key = "skmem";
        analyzer = new AbstractMultiValueTunnelSocketStatsAnalyzer(key, "\\s*,\\s*") {
            @Override
            protected double getValueFromString(String[] values) {
                return parseDouble(values[3]);
            }

            @Override
            public String getType() {
                return key;
            }
        };
        double result = analyzer.analyze(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
                "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114"));
        logger.info("result: {}", result);
        Assert.assertEquals(130560, result, 0);
    }
}
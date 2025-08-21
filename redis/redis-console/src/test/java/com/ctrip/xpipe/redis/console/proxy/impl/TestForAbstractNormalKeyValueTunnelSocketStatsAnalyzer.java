package com.ctrip.xpipe.redis.console.proxy.impl;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer {

    private Logger logger = LoggerFactory.getLogger(TestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer.class);

    private AbstractNormalKeyValueTunnelSocketStatsAnalyzer analyzer;

    private String key;

    @Before
    public void beforeTestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer() {
        key = "cwnd";
        analyzer = new AbstractNormalKeyValueTunnelSocketStatsAnalyzer(key) {
            @Override
            public String getType() {
                return key;
            }
        };
    }


    @Test
    public void analyze() {
        double result = analyzer.analyze(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
                "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114"));
        Assert.assertEquals(12, result, 0);
    }

    @Test
    public void testSend() {
        key = "send";
        analyzer = new AbstractNormalKeyValueTunnelSocketStatsAnalyzer(key) {
            @Override
            public String getType() {
                return key;
            }

            @Override
            protected double getValue(String value) {
                double number = parseDouble(value);
                int multiple = getMultiple(value);
                return number * multiple;
            }

            private int getMultiple(String value) {
                if(value.contains("K")) {
                    return 1000;
                } else if(value.contains("M")) {
                    return 1000 * 1000;
                }
                return 1;
            }
        };
        double result = analyzer.analyze(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.114.98:443 ::ffff:10.8.63.135:28153 ",
                "skmem:(r0,rb374400,t0,tb130560,f0,w0,o0,bl0) bbr wscale:10,10 rto:457 rtt:256.633/14.967 ato:40 mss:1460 cwnd:12 send 546.1Kbps retrans:0/9052 rcv_rtt:517679 rcv_space:48114"));
        logger.info("result: {}", result);
        Assert.assertEquals(546000, result, 0);
    }

    @Test
    public void testParseLong() {
        Assert.assertEquals(9527, analyzer.parseDouble("9527"), 0);
        Assert.assertEquals(9527, analyzer.parseDouble("sdfas9527"), 0);
        Assert.assertEquals(9527.098, analyzer.parseDouble("9527.098"), 1);
        Assert.assertEquals(9527.1293, analyzer.parseDouble("sdfsdgda9527.1293"), 1);
        Assert.assertEquals(0.9527, analyzer.parseDouble("0.9527Kbps"), 1);
    }

}
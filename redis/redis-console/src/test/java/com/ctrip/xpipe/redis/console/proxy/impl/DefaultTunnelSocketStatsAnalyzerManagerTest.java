package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzer;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class DefaultTunnelSocketStatsAnalyzerManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultTunnelSocketStatsAnalyzerManager manager;


    @Test
    public void testAnalyze() {
        List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> result = manager.analyze(new DefaultProxyChain("FRA-AWS", "cluster", "shard", "sharb", Lists.newArrayList(
                tunnelInfo1(), tunnelInfo2()
        )));
        int index = 1;
        for(TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics metric : result) {
            logger.info("tunnel-{}", index++);
            logger.info("frontend:");
            logger.info("{}: {}", metric.getFrontend().getMetricType(), metric.getFrontend().getValue());
            logger.info("backend:");
            logger.info("{}: {}", metric.getBackend().getMetricType(), metric.getBackend().getValue());
        }
    }

    private DefaultTunnelInfo tunnelInfo1() {
        DefaultTunnelInfo info = new DefaultTunnelInfo(new ProxyModel().setDcName("oy").setHostPort(HostPort.fromString("10.2.131.201:47082")),
                "10.2.131.242:55306-R(10.2.131.200:53868)-L(10.2.131.201:47082)->R(10.2.73.170:6389)-TCP://10.2.73.170:6389");
        info.setTunnelSocketStatsResult(new TunnelSocketStatsResult(info.getTunnelId(),
                new SocketStatsResult(Lists.newArrayList("ESTAB 0 285 ::ffff:10.28.81.65:443 ::ffff:10.8.63.145:54510 ", "skmem:(r0,rb374400,t0,tb130560,f1251,w2845,o0,bl0) bbr wscale:10,10 rto:459 rtt:258.008/0.348 ato:40 mss:1460 cwnd:11 send 498.0Kbps unacked:2 retrans:0/3119 rcv_rtt:517020 rcv_space:40824")),
                new SocketStatsResult(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.81.65:36190 ::ffff:10.28.97.195:5379 ", "skmem:(r0,rb566888,t0,tb130560,f4096,w0,o0,bl0) bbr wscale:7,10 rto:203 rtt:2.043/3.712 ato:87 mss:1448 cwnd:11 send 62.4Mbps rcv_rtt:1 rcv_space:57729"))));
        info.setTunnelStatsResult(new TunnelStatsResult(info.getTunnelId(), "established", System.currentTimeMillis(), System.currentTimeMillis(), HostPort.fromString("10.2.131.201:47082"), HostPort.fromString("10.2.131.201:47082")));
        return info;
    }

    private DefaultTunnelInfo tunnelInfo2() {
        DefaultTunnelInfo info = new DefaultTunnelInfo(new ProxyModel().setDcName("fra-aws").setHostPort(new HostPort("10.2.131.200", 53868)),
                "10.2.131.242:55306-R(10.2.131.242:55306)-L(10.2.131.200:53868)->R(10.2.131.201:443-TCP://10.2.73.170:6389");
        info.setTunnelSocketStatsResult(new TunnelSocketStatsResult(info.getTunnelId(),
                new SocketStatsResult(Lists.newArrayList("ESTAB 0 285 ::ffff:10.28.81.65:443 ::ffff:10.8.63.145:54510 ", "skmem:(r0,rb374400,t0,tb130560,f1251,w2845,o0,bl0) bbr wscale:10,10 rto:459 rtt:258.008/0.348 ato:40 mss:1460 cwnd:11 send 498.0Kbps unacked:2 retrans:0/3119 rcv_rtt:517020 rcv_space:40824")),
                new SocketStatsResult(Lists.newArrayList("ESTAB 0 0 ::ffff:10.28.81.65:36190 ::ffff:10.28.97.195:5379 ", "skmem:(r0,rb566888,t0,tb130560,f4096,w0,o0,bl0) bbr wscale:7,10 rto:203 rtt:2.043/3.712 ato:87 mss:1448 cwnd:11 send 62.4Mbps rcv_rtt:1 rcv_space:57729"))));
        info.setTunnelStatsResult(new TunnelStatsResult(info.getTunnelId(), "established", System.currentTimeMillis(), System.currentTimeMillis(), HostPort.fromString("10.2.131.201:47082"), HostPort.fromString("10.2.131.201:47082")));
        return info;
    }
}
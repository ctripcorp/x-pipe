package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2021/6/24
 */
public class DefaultProxyChainTest extends AbstractConsoleTest {

    private ProxyModel proxyModel;

    @Before
    public void setupDefaultProxyChainTest() {
        proxyModel = new ProxyModel();
        proxyModel.setActive(true).setUri("PROXYTCP://127.0.0.1:80").setId(1)
                .setMonitorActive(false).setDcName("jq");
    }

    @Test
    public void testTunnelMissStatsResult() {
        DefaultTunnelInfo tunnelInfo1 = new DefaultTunnelInfo(proxyModel, "test-tunnel1");
        DefaultTunnelInfo tunnelInfo2 = new DefaultTunnelInfo(proxyModel, "test-tunnel2");
        tunnelInfo2.setTunnelStatsResult(new TunnelStatsResult("test-tunnel2", "Tunnel-Established", 0, 0, new HostPort(), new HostPort()));
        DefaultProxyChain chain = new DefaultProxyChain("oy", "cluster1", "shard1", "sharb", Arrays.asList(tunnelInfo1, tunnelInfo2));

        ProxyTunnelInfo proxyTunnelInfo = chain.buildProxyTunnelInfo();
        Assert.assertEquals(1, proxyTunnelInfo.getTunnelStatsInfos().size());
        Assert.assertEquals("test-tunnel2", proxyTunnelInfo.getTunnelStatsInfos().get(0).getTunnelId());
    }

    @Test
    public void testClone() {
        DefaultProxyChain proxyChain = Codec.DEFAULT.decode("{\"backupDcId\":\"SIN-AWS\",\"clusterId\":\"GSPoiCache2\",\"shardId\":\"GSPoiCache2_v20220519_2\",\"peerDcId\":\"SHARB\",\"tunnelInfos\":[{\"tunnelDcId\":\"SHARB\",\"tunnelId\":\"10.129.68.65:48722-R(10.60.204.185:48459)-L(10.57.195.124:38060)->R(10.99.132.31:6454)-TCP://10.99.132.31:6454\",\"proxyModel\":{\"uri\":\"PROXYTCP://10.57.195.124:80\",\"dcName\":\"SHARB\",\"id\":325,\"active\":true,\"monitorActive\":true,\"hostPort\":{\"port\":80,\"host\":\"10.57.195.124\"}},\"tunnelStatsResult\":{\"tunnelId\":\"10.129.68.65:48722-R(10.60.204.185:48459)-L(10.57.195.124:38060)->R(10.99.132.31:6454)-TCP://10.99.132.31:6454\",\"tunnelState\":\"Tunnel-Established\",\"frontend\":{\"port\":443,\"host\":\"10.57.195.124\"},\"backend\":{\"port\":38060,\"host\":\"10.57.195.124\"},\"protocolRecvTime\":1703576919127,\"protocolSndTime\":1703576919127,\"closeTime\":-1,\"closeFrom\":\"Not Yet\"},\"tunnelSocketStatsResult\":{\"tunnelId\":\"10.129.68.65:48722-R(10.60.204.185:48459)-L(10.57.195.124:38060)->R(10.99.132.31:6454)-TCP://10.99.132.31:6454\",\"frontendSocketStats\":{\"result\":[\"ESTAB      0      105       ::ffff:10.57.195.124:443                   ::ffff:10.60.204.185:48459              \",\"  skmem:(r0,rb374400,t0,tb2624256,f1792,w2304,o0,bl0,d145) bbr wscale:10,10 rto:274 rtt:73.701/0.448 ato:40 mss:1460 rcvmss:1036 advmss:1460 cwnd:34 bytes_acked:5385278559 bytes_received:17669224 segs_out:29874924 segs_in:24061408 data_segs_out:29813172 data_segs_in:242106 bbr:(bw:6.8Mbps,mrtt:73.342,pacing_gain:1.25,cwnd_gain:2) send 5.4Mbps lastsnd:51 lastrcv:25 lastack:171 pacing_rate 7.0Mbps delivery_rate 6.8Mbps app_limited unacked:1 retrans:0/106570 reordering:4 rcv_rtt:380909 rcv_space:29358 minrtt:73.336\"],\"timestamp\":1703818983031},\"backendSocketStats\":{\"result\":[\"ESTAB      0      27        ::ffff:10.57.195.124:38060              ::ffff:10.99.132.31:6454               \",\"  skmem:(r0,rb1890616,t0,tb130560,f1792,w2304,o0,bl0,d2463) bbr wscale:7,10 rto:217 rtt:16.18/11.05 ato:42 mss:1448 rcvmss:1448 advmss:1448 cwnd:10 bytes_acked:6534735 bytes_received:6051294796 segs_out:23121506 segs_in:32093607 data_segs_out:242026 data_segs_in:32031120 bbr:(bw:1158.4Mbps,mrtt:0.07,pacing_gain:2.88672,cwnd_gain:2.88672) send 7.2Mbps lastsnd:24 lastrcv:50 lastack:312 pacing_rate 3464.1Mbps delivery_rate 1158.4Mbps app_limited unacked:1 rcv_rtt:27.75 rcv_space:221184 minrtt:0.06\"],\"timestamp\":1703818983031}},\"tunnelTrafficResult\":{\"tunnelId\":\"10.129.68.65:48722-R(10.60.204.185:48459)-L(10.57.195.124:38060)->R(10.99.132.31:6454)-TCP://10.99.132.31:6454\",\"frontend\":{\"timestamp\":1703818983419,\"inputBytes\":10649220,\"outputBytes\":4509204672,\"inputRates\":55,\"outputRates\":7271},\"backend\":{\"timestamp\":1703818983419,\"inputBytes\":6051298860,\"outputBytes\":6534761,\"inputRates\":9732,\"outputRates\":33}}}]}",
                DefaultProxyChain.class);
        DefaultProxyChain cloneProxyChain = proxyChain.clone();
        Assert.assertEquals(Codec.DEFAULT.encode(proxyChain), Codec.DEFAULT.encode(cloneProxyChain));
        Assert.assertEquals(proxyChain.toString(), cloneProxyChain.toString());
    }

}

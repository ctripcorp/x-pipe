package com.ctrip.xpipe.redis.console.proxy.impl;

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

}

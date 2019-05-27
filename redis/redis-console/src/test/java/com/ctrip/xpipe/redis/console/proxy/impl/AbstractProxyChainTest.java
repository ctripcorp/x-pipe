package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.google.common.collect.Lists;


public class AbstractProxyChainTest extends AbstractRedisTest {

    private HostPort frontend = HostPort.fromString("10.26.188.107:47862");

    private HostPort backend = HostPort.fromString("10.26.188.107:80");

    protected ProxyModel getProxy(String dcId) {
        return new ProxyModel().setDcName(dcId);
    }

    protected TunnelSocketStatsResult genTunnelSSR(String tunnelId) {
        return new TunnelSocketStatsResult(tunnelId, new SocketStatsResult(Lists.newArrayList()), new SocketStatsResult(Lists.newArrayList()));
    }

    protected TunnelStatsResult genTunnelSR(String tunnelId) {
        return new TunnelStatsResult(tunnelId, "Established", System.currentTimeMillis() - 1000 * 60, System.currentTimeMillis() - 100 * 60 - 10, frontend, backend);
    }

    private String getSource() {
        return "127.0.0.1:1880";
    }

    private String getDest() {
        return "TCP://127.0.0.3:6380";
    }

    protected String generateTunnelId() {
        return String.format("%s-%s-%s", getSource(), generateCurrentTunnel(), getDest());
    }

    private String generateCurrentTunnel() {
        int last = randomInt(5, 10);
        return String.format("L(127.0.0.%d:80)-R(127.0.0.%d:%d)", last, last, randomPort());
    }
}

package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;

import java.util.List;

public class ProxyPingStatsModel {

    private ProxyModel model;

    private List<PingStatsResult> pingStatsResults;

    public ProxyPingStatsModel() {
    }

    public ProxyPingStatsModel(ProxyModel model, List<PingStatsResult> pingStatsResults) {
        this.model = model;
        this.pingStatsResults = pingStatsResults;
    }

    public ProxyModel getProxyModel() {
        return model;
    }

    public ProxyPingStatsModel setProxyModel(ProxyModel model) {
        this.model = model;
        return this;
    }

    public List<PingStatsResult> getPingStatsResults() {
        return pingStatsResults;
    }

    public ProxyPingStatsModel setPingStatsResults(List<PingStatsResult> pingStatsResults) {
        this.pingStatsResults = pingStatsResults;
        return this;
    }
}

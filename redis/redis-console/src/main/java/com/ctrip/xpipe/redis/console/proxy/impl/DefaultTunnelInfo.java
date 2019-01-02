package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;

import java.util.Objects;

public class DefaultTunnelInfo implements TunnelInfo {

    private String dcId;

    private String tunnelId;

    private ProxyModel proxyModel;

    private TunnelStatsResult tunnelStatsResult;

    private TunnelSocketStatsResult socketStatsResult;

    private TunnelTrafficResult tunnelTrafficResult;

    public DefaultTunnelInfo(ProxyModel proxyModel, String tunnelId) {
        this.proxyModel = proxyModel;
        this.dcId = proxyModel.getDcName();
        this.tunnelId = tunnelId;
    }

    @Override
    public String getTunnelDcId() {
        return dcId;
    }

    @Override
    public String getTunnelId() {
        return tunnelId;
    }

    @Override
    public ProxyModel getProxyModel() {
        return proxyModel;
    }

    @Override
    public TunnelStatsResult getTunnelStatsResult() {
        return tunnelStatsResult;
    }

    @Override
    public TunnelSocketStatsResult getTunnelSocketStatsResult() {
        return socketStatsResult;
    }

    @Override
    public TunnelTrafficResult getTunnelTrafficResult() {
        return tunnelTrafficResult;
    }

    public DefaultTunnelInfo setTunnelStatsResult(TunnelStatsResult tunnelStatsResult) {
        this.tunnelStatsResult = tunnelStatsResult;
        return this;
    }

    public DefaultTunnelInfo setSocketStatsResult(TunnelSocketStatsResult socketStatsResult) {
        this.socketStatsResult = socketStatsResult;
        return this;
    }

    public DefaultTunnelInfo setTunnelTrafficResult(TunnelTrafficResult tunnelTrafficResult) {
        this.tunnelTrafficResult = tunnelTrafficResult;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultTunnelInfo that = (DefaultTunnelInfo) o;
        return Objects.equals(tunnelId, that.tunnelId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tunnelId);
    }

    @Override
    public String toString() {
        return "DefaultTunnelInfo{" +
                "dcId='" + dcId + '\'' +
                ", tunnelId='" + tunnelId + '\'' +
                ", proxyModel=" + proxyModel.toString() +
                ", tunnelStatsResult=" + tunnelStatsResult.toString() +
                ", socketStatsResult=" + socketStatsResult.toString() +
                '}';
    }
}

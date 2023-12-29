package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;

import java.io.Serializable;
import java.util.Objects;

public class DefaultTunnelInfo implements TunnelInfo, Serializable {

    private String tunnelDcId;

    private String tunnelId;

    private ProxyModel proxyModel;

    private TunnelStatsResult tunnelStatsResult;

    private TunnelSocketStatsResult tunnelSocketStatsResult;

    private TunnelTrafficResult tunnelTrafficResult;

    public DefaultTunnelInfo() {
    }

    public DefaultTunnelInfo(ProxyModel proxyModel, String tunnelId) {
        this.proxyModel = proxyModel;
        this.tunnelDcId = proxyModel.getDcName();
        this.tunnelId = tunnelId;
    }

    @Override
    public String getTunnelDcId() {
        return tunnelDcId;
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
        return tunnelSocketStatsResult;
    }

    @Override
    public TunnelTrafficResult getTunnelTrafficResult() {
        return tunnelTrafficResult;
    }

    public DefaultTunnelInfo setTunnelStatsResult(TunnelStatsResult tunnelStatsResult) {
        this.tunnelStatsResult = tunnelStatsResult;
        return this;
    }

    public DefaultTunnelInfo setTunnelSocketStatsResult(TunnelSocketStatsResult tunnelSocketStatsResult) {
        this.tunnelSocketStatsResult = tunnelSocketStatsResult;
        return this;
    }

    public DefaultTunnelInfo setTunnelTrafficResult(TunnelTrafficResult tunnelTrafficResult) {
        this.tunnelTrafficResult = tunnelTrafficResult;
        return this;
    }

    public DefaultTunnelInfo setTunnelDcId(String tunnelDcId) {
        this.tunnelDcId = tunnelDcId;
        return this;
    }

    public DefaultTunnelInfo setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
        return this;
    }

    public DefaultTunnelInfo setProxyModel(ProxyModel proxyModel) {
        this.proxyModel = proxyModel;
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
    protected DefaultTunnelInfo clone() {
        DefaultTunnelInfo clone = new DefaultTunnelInfo();
        clone.tunnelId = this.tunnelId;
        clone.tunnelDcId = this.tunnelDcId;

        if (null != this.proxyModel) {
            clone.proxyModel = this.proxyModel.clone();
        }

        if (null != this.tunnelSocketStatsResult) {
            clone.tunnelSocketStatsResult = this.tunnelSocketStatsResult.clone();
        }

        if (null != this.tunnelStatsResult) {
            clone.tunnelStatsResult = this.tunnelStatsResult.clone();
        }

        if (null != this.tunnelTrafficResult) {
            clone.tunnelTrafficResult = this.tunnelTrafficResult.clone();
        }

        return clone;
    }

    @Override
    public String toString() {
        return "DefaultTunnelInfo{" +
                "dcId='" + tunnelDcId + '\'' +
                ", tunnelId='" + tunnelId + '\'' +
                ", proxyModel=" + proxyModel.toString() +
                ", tunnelStatsResult=" + tunnelStatsResult.toString() +
                ", socketStatsResult=" + tunnelSocketStatsResult.toString() +
                '}';
    }
}

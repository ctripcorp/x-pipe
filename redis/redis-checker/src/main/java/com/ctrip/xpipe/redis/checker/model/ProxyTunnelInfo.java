package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2021/3/11
 */
public class ProxyTunnelInfo {

    private String backupDcId;

    private String clusterId;

    private String shardId;

    //TODO ADD peerDcId when dealing with proxy health.

    private List<TunnelStatsInfo> tunnelStatsInfos;

    public ProxyTunnelInfo() {

    }

    public String getBackupDcId() {
        return backupDcId;
    }

    public ProxyTunnelInfo setBackupDcId(String backupDcId) {
        this.backupDcId = backupDcId;
        return this;
    }

    public String getClusterId() {
        return clusterId;
    }

    public ProxyTunnelInfo setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public String getShardId() {
        return shardId;
    }

    public ProxyTunnelInfo setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public List<TunnelStatsInfo> getTunnelStatsInfos() {
        return tunnelStatsInfos;
    }

    public ProxyTunnelInfo setTunnelStatsInfos(List<TunnelStatsInfo> tunnelStatsInfos) {
        this.tunnelStatsInfos = tunnelStatsInfos;
        return this;
    }

    public List<HostPort> getBackends() {
        List<HostPort> backends = Lists.newArrayList();
        for (TunnelStatsInfo tunnelInfo : tunnelStatsInfos) {
            HostPort backend = tunnelInfo.getBackend();
            HostPort proxy = tunnelInfo.getProxyHost();
            backends.add(new HostPort(proxy.getHost(), backend.getPort()));
        }

        return backends;
    }

    @Override
    public String toString() {
        return "ProxyTunnelInfo{" +
                "backupDcId='" + backupDcId + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", shardId='" + shardId + '\'' +
                ", tunnelStatsInfos=" + tunnelStatsInfos +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProxyTunnelInfo that = (ProxyTunnelInfo) o;
        return Objects.equals(backupDcId, that.backupDcId) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(shardId, that.shardId) &&
                Objects.equals(tunnelStatsInfos, that.tunnelStatsInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backupDcId, clusterId, shardId, tunnelStatsInfos);
    }
}

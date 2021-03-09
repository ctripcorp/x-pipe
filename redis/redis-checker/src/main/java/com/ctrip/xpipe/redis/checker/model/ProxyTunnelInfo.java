package com.ctrip.xpipe.redis.checker.model;

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

    private List<TunnelStatsInfo> tunnelStatsInfos;

    public String getBackupDcId() {
        return backupDcId;
    }

    public void setBackupDcId(String backupDcId) {
        this.backupDcId = backupDcId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public List<TunnelStatsInfo> getTunnelStatsInfos() {
        return tunnelStatsInfos;
    }

    public void setTunnelStatsInfos(List<TunnelStatsInfo> tunnelStatsInfos) {
        this.tunnelStatsInfos = tunnelStatsInfos;
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

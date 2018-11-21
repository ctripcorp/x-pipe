package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

import java.util.Objects;

public class TunnelModel {

    private String tunnelId;

    private String backupDcId;

    private String clusterId;

    private String shardId;

    private TunnelInfo tunnelInfo;

    public TunnelModel(String tunnelId, String backupDcId, String clusterId, String shardId, TunnelInfo tunnelInfo) {
        this.tunnelId = tunnelId;
        this.backupDcId = backupDcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.tunnelInfo = tunnelInfo;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public TunnelModel setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
        return this;
    }

    public String getBackupDcId() {
        return backupDcId;
    }

    public TunnelModel setBackupDcId(String backupDcId) {
        this.backupDcId = backupDcId;
        return this;
    }

    public String getClusterId() {
        return clusterId;
    }

    public TunnelModel setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public String getShardId() {
        return shardId;
    }

    public TunnelModel setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public TunnelInfo getTunnelInfo() {
        return tunnelInfo;
    }

    public TunnelModel setTunnelInfo(TunnelInfo tunnelInfo) {
        this.tunnelInfo = tunnelInfo;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelModel that = (TunnelModel) o;
        return Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(backupDcId, that.backupDcId) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(shardId, that.shardId) &&
                Objects.equals(tunnelInfo, that.tunnelInfo);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tunnelId, backupDcId, clusterId, shardId, tunnelInfo);
    }
}

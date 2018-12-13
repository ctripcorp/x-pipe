package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;

import java.util.Objects;

public class TunnelModel {

    private String tunnelId;

    private String backupDcId;

    private String clusterId;

    private String shardId;

    private TunnelStatsResult tunnelStatsResult;

    private TunnelSocketStatsMetricOverview socketStatsMetricOverview;

    public TunnelModel(String tunnelId, String backupDcId, String clusterId, String shardId,
                       TunnelStatsResult tunnelStatsResult, TunnelSocketStatsMetricOverview socketStatsMetricOverview) {
        this.tunnelId = tunnelId;
        this.backupDcId = backupDcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.tunnelStatsResult = tunnelStatsResult;
        this.socketStatsMetricOverview = socketStatsMetricOverview;
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

    public TunnelStatsResult getTunnelStatsResult() {
        return tunnelStatsResult;
    }

    public TunnelModel setTunnelStatsResult(TunnelStatsResult tunnelStatsResult) {
        this.tunnelStatsResult = tunnelStatsResult;
        return this;
    }

    public TunnelSocketStatsMetricOverview getSocketStatsMetricOverview() {
        return socketStatsMetricOverview;
    }

    public TunnelModel setSocketStatsMetricOverview(TunnelSocketStatsMetricOverview socketStatsMetricOverview) {
        this.socketStatsMetricOverview = socketStatsMetricOverview;
        return this;
    }

}

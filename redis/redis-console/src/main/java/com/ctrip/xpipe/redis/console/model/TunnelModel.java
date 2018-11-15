package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

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
}

package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

import java.util.Arrays;
import java.util.List;

public class DefaultProxyChain implements ProxyChain {

    private String backupDcId;

    private String clusterId;

    private String shardId;

    private List<TunnelInfo> tunnelInfos;

    public DefaultProxyChain(String backupDcId, String clusterId, String shardId, List<TunnelInfo> tunnelInfos) {
        this.backupDcId = backupDcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.tunnelInfos = tunnelInfos;
    }

    @Override
    public String getBackupDc() {
        return backupDcId;
    }

    @Override
    public String getCluster() {
        return clusterId;
    }

    @Override
    public String getShard() {
        return shardId;
    }

    @Override
    public List<TunnelInfo> getTunnels() {
        return tunnelInfos;
    }

    @Override
    public String toString() {
        return "DefaultProxyChain{" +
                "backupDcId='" + backupDcId + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", shardId='" + shardId + '\'' +
                ", tunnelInfos=" + Arrays.deepToString(tunnelInfos.toArray(new TunnelInfo[0])) +
                '}';
    }
}

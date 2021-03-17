package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.model.TunnelStatsInfo;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;

import java.util.ArrayList;
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
    public ProxyTunnelInfo buildProxyTunnelInfo() {
        ProxyTunnelInfo proxyTunnelInfo = new ProxyTunnelInfo();
        List<TunnelStatsInfo> tunnelStatsInfos = new ArrayList<>();

        tunnelInfos.forEach(tunnelInfo -> {
            TunnelStatsInfo tunnelStatsInfo = new TunnelStatsInfo();
            TunnelStatsResult statsResult = tunnelInfo.getTunnelStatsResult();
            tunnelStatsInfo.setBackend(statsResult.getBackend())
                    .setFrontend(statsResult.getFrontend())
                    .setTunnelId(statsResult.getTunnelId())
                    .setProxyHost(tunnelInfo.getProxyModel().getHostPort());

            tunnelStatsInfos.add(tunnelStatsInfo);
        });

        proxyTunnelInfo.setBackupDcId(backupDcId).setClusterId(clusterId)
                .setShardId(shardId).setTunnelStatsInfos(tunnelStatsInfos);
        return proxyTunnelInfo;
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

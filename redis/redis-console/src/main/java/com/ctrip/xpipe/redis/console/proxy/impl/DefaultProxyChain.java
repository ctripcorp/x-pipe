package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.model.TunnelStatsInfo;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultProxyChain implements ProxyChain {

    private String backupDcId;

    private String clusterId;

    private String shardId;

    private String peerDcId;

    private List<DefaultTunnelInfo> tunnelInfos;

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyChain.class);

    public DefaultProxyChain() {
    }

    public DefaultProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId, List<DefaultTunnelInfo> tunnelInfos) {
        this.backupDcId = backupDcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.peerDcId = peerDcId;
        this.tunnelInfos = tunnelInfos;
    }

    @Override
    public String getBackupDcId() {
        return backupDcId;
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public String getShardId() { return shardId; }

    @Override
    public String getPeerDcId() {return peerDcId;}

    @Override
    public List<DefaultTunnelInfo> getTunnelInfos() {
        return tunnelInfos;
    }

    public DefaultProxyChain setBackupDcId(String backupDcId) {
        this.backupDcId = backupDcId;
        return this;
    }

    public DefaultProxyChain setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public DefaultProxyChain setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public DefaultProxyChain setPeerDcId(String peerDcId) {
        this.peerDcId = peerDcId;
        return this;
    }

    public DefaultProxyChain setTunnelInfos(List<DefaultTunnelInfo> tunnelInfos) {
        this.tunnelInfos = tunnelInfos;
        return this;
    }

    @Override
    public ProxyTunnelInfo buildProxyTunnelInfo() {
        ProxyTunnelInfo proxyTunnelInfo = new ProxyTunnelInfo();
        List<TunnelStatsInfo> tunnelStatsInfos = new ArrayList<>();

        tunnelInfos.forEach(tunnelInfo -> {
            TunnelStatsResult statsResult = tunnelInfo.getTunnelStatsResult();
            if (null == statsResult) {
                logger.info("[buildProxyTunnelInfo] tunnel miss stats {}", tunnelInfo);
                return;
            }

            TunnelStatsInfo tunnelStatsInfo = new TunnelStatsInfo();
            tunnelStatsInfo.setBackend(statsResult.getBackend())
                    .setFrontend(statsResult.getFrontend())
                    .setTunnelId(statsResult.getTunnelId())
                    .setProxyHost(tunnelInfo.getProxyModel().getHostPort());

            tunnelStatsInfos.add(tunnelStatsInfo);
        });

        proxyTunnelInfo.setBackupDcId(backupDcId).setClusterId(clusterId)
                .setShardId(shardId).setTunnelStatsInfos(tunnelStatsInfos)
                .setPeerDcId(peerDcId);
        return proxyTunnelInfo;
    }

    @Override
    public DefaultProxyChain clone() {
        DefaultProxyChain clone = new DefaultProxyChain();
        clone.peerDcId = this.peerDcId;
        clone.backupDcId = this.backupDcId;
        clone.clusterId = this.clusterId;
        clone.shardId = this.shardId;

        if (null != this.tunnelInfos) {
            List<DefaultTunnelInfo> cloneTunnelInfos = Lists.newArrayList();
            for (DefaultTunnelInfo tunnelInfo: this.tunnelInfos) {
                cloneTunnelInfos.add(tunnelInfo.clone());
            }
            clone.tunnelInfos = cloneTunnelInfos;
        }

        return clone;
    }

    @Override
    public String toString() {
        return "DefaultProxyChain{" +
                "backupDcId='" + backupDcId + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", shardId='" + shardId + '\'' +
                ", peerDcId='" + peerDcId + '\'' +
                ", tunnelInfos=" + tunnelInfos +
                '}';
    }
}

package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;

import java.util.List;

public class ProxyChainModel {

    private String shardId;

    private DefaultTunnelInfo activeDcTunnel;

    private DefaultTunnelInfo optionalTunnel;

    private DefaultTunnelInfo backupDcTunnel;

    public ProxyChainModel(ProxyChain chain, String activeDcId, String backupDcId) {
        this.shardId = chain.getShardId();
        List<DefaultTunnelInfo> tunnels = chain.getTunnelInfos();
        for(DefaultTunnelInfo info : tunnels) {
            if(info.getProxyModel().getDcName().equalsIgnoreCase(activeDcId)) {
                this.activeDcTunnel = info;
            } else if(info.getProxyModel().getDcName().equalsIgnoreCase(backupDcId)){
                this.backupDcTunnel = info;
            } else {
                this.optionalTunnel = info;
            }
        }
    }

    public String getShardId() {
        return shardId;
    }

    public DefaultTunnelInfo getBackupDcTunnel() {
        return backupDcTunnel;
    }

    public DefaultTunnelInfo getOptionalTunnel() {
        return optionalTunnel;
    }

    public DefaultTunnelInfo getActiveDcTunnel() {
        return activeDcTunnel;
    }
}

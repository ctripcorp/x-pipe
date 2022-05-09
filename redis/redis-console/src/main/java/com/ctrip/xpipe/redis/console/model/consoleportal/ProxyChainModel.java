package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

import java.util.List;

public class ProxyChainModel {

    private String shardId;

    private TunnelInfo activeDcTunnel;

    private TunnelInfo optionalTunnel;

    private TunnelInfo backupDcTunnel;

    public ProxyChainModel(ProxyChain chain, String activeDcId, String backupDcId) {
        this.shardId = chain.getShard();
        List<TunnelInfo> tunnels = chain.getTunnels();
        for(TunnelInfo info : tunnels) {
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

    public TunnelInfo getBackupDcTunnel() {
        return backupDcTunnel;
    }

    public TunnelInfo getOptionalTunnel() {
        return optionalTunnel;
    }

    public TunnelInfo getActiveDcTunnel() {
        return activeDcTunnel;
    }
}

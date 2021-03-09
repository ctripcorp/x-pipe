package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/11
 */
@Component
public class CheckerProxyManager implements ProxyManager, LeaderAware {

    // TODO: load proxy info from console

    @Override
    public ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId) {
        return null;
    }

    @Override
    public void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo) {

    }

    @Override
    public void isleader() {

    }

    @Override
    public void notLeader() {

    }
}

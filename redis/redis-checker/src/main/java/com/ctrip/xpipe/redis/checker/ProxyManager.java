package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;

/**
 * @author lishanglin
 * date 2021/3/11
 */
public interface ProxyManager {

    ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId);

    void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo);

}

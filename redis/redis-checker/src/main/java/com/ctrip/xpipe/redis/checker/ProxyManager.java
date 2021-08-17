package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/11
 */
public interface ProxyManager {

    List<ProxyTunnelInfo> getAllProxyTunnels();

    ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId, String peerDcId);

    void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo);

}

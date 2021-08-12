package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;

import java.util.List;

public interface ProxyChain {

    String getBackupDc();

    String getCluster();

    String getShard();

    String getPeerDcId();

    List<TunnelInfo> getTunnels();

    ProxyTunnelInfo buildProxyTunnelInfo();

}

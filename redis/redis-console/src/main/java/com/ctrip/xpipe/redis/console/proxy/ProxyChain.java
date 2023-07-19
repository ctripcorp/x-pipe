package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;

import java.io.Serializable;
import java.util.List;

public interface ProxyChain extends Serializable {

    String getBackupDcId();

    String getClusterId();

    String getShardId();

    String getPeerDcId();

    List<DefaultTunnelInfo> getTunnelInfos();

    ProxyTunnelInfo buildProxyTunnelInfo();

}

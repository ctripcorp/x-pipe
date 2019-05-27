package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;

public interface TunnelInfo {

    String getTunnelDcId();

    String getTunnelId();

    ProxyModel getProxyModel();

    TunnelStatsResult getTunnelStatsResult();

    TunnelSocketStatsResult getTunnelSocketStatsResult();

    TunnelTrafficResult getTunnelTrafficResult();
}

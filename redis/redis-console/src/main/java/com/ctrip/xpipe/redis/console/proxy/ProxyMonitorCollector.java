package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;

import java.util.List;

public interface ProxyMonitorCollector extends Startable, Stoppable {

    ProxyModel getProxyInfo();

    List<PingStatsResult> getPingStatsResults();

    List<TunnelStatsResult> getTunnelStatsResults();

    List<TunnelSocketStatsResult> getTunnelSocketStatsResults();

    List<TunnelInfo> getTunnelInfos();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    interface Listener {
        void ackPingStatsResult(List<PingStatsResult> realTimeResults);
    }
}

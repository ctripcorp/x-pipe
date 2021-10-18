package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.model.ProxyModel;

import java.util.List;

public interface ProxyMonitorCollectorManager extends ConsoleLeaderAware {

    ProxyMonitorCollector getOrCreate(ProxyModel proxyModel);

    List<ProxyMonitorCollector> getProxyMonitorResults();

    void remove(ProxyModel proxyModel);

}

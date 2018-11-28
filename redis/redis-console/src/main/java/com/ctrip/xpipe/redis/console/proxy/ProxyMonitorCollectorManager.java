package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.console.model.ProxyModel;

import java.util.List;

public interface ProxyMonitorCollectorManager extends CrossDcLeaderAware {

    ProxyMonitorCollector getOrCreate(ProxyModel proxyModel);

    List<ProxyMonitorCollector> getProxyMonitorResults();

    void remove(ProxyModel proxyModel);

}

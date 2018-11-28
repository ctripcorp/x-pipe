package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.console.model.ProxyModel;

import java.util.List;

public interface ProxyMonitorCollectorManager {

    ProxyMonitorCollector getOrCreate(ProxyModel proxyModel);

    List<ProxyMonitorCollector> getProxyMonitorResults();

    void remove(ProxyModel proxyModel);

}

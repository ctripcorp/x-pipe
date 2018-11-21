package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.console.model.ProxyModel;

import java.util.List;

public interface ProxyMonitorCollectorManager extends ProxyMonitorCollector.Listener {

    ProxyMonitorCollector getOrCreate(ProxyModel proxyModel);

    List<ProxyMonitorCollector> getProxyMonitorResults();

    void remove(ProxyModel proxyModel);

    void register(Listener listener);

    void stopNotify(Listener listener);

    interface Listener {

        void onGlobalEvent(ProxyMonitorCollectType type);

        void onLocalEvent(ProxyMonitorCollectType type, ProxyModel proxyModel);
    }

    enum ProxyMonitorCollectType {
        UPDATE, CREATE, DELETE
    }
}

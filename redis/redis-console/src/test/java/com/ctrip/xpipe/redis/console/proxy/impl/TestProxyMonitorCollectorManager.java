package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Lists;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProxyMonitorCollectorManager extends AbstractRedisTest implements ProxyMonitorCollectorManager {
    @Override
    public ProxyMonitorCollector getOrCreate(ProxyModel proxyModel) {
        try {
            return new DefaultProxyMonitorCollector(scheduled, getXpipeNettyClientKeyedObjectPool(), proxyModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<ProxyMonitorCollector> getProxyMonitorResults() {
        return Lists.newArrayList();
    }

    @Override
    public void remove(ProxyModel proxyModel) {

    }

    @Override
    public void register(Listener listener) {

    }

    @Override
    public void stopNotify(Listener listener) {

    }

    @Override
    public void onChange(ProxyMonitorCollector collector) {

    }
}

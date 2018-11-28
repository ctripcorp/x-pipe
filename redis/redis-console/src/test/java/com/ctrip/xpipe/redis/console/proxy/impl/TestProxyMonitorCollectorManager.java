package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProxyMonitorCollectorManager extends AbstractProxyChainTest implements ProxyMonitorCollectorManager {

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2);

    @Autowired
    private ProxyChainAnalyzer analyzer;

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
        try {
            DefaultProxyMonitorCollector collector1 = mock(DefaultProxyMonitorCollector.class);
            when(collector1.getProxyInfo()).thenReturn(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)).setUri("TCP://127.0.0.1:443"));
            when(collector1.getTunnelInfos()).thenReturn(Lists.newArrayList(analyzer.getProxyChain("fra", "cluster1", "shard1").getTunnels().get(0), analyzer.getProxyChain("fra", "cluster1", "shard2").getTunnels().get(0)));
            when(collector1.getPingStatsResults()).thenReturn(Lists.newArrayList());

            DefaultProxyMonitorCollector collector2 = mock(DefaultProxyMonitorCollector.class);
            when(collector2.getProxyInfo()).thenReturn(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)).setUri("TCP://127.0.0.3:80"));
            when(collector2.getTunnelInfos()).thenReturn(Lists.newArrayList(analyzer.getProxyChain("fra", "cluster1", "shard1").getTunnels().get(1), analyzer.getProxyChain("fra", "cluster1", "shard2").getTunnels().get(1)));
            when(collector2.getPingStatsResults()).thenReturn(
                    Lists.newArrayList(new PingStatsResult(System.currentTimeMillis() - 1000 * 60 - 10, System.currentTimeMillis() - 1000 * 60, new HostPort("127.0.0.1", 443), new HostPort("192.168.0.1", 443)),
                            new PingStatsResult(System.currentTimeMillis() - 1000 * 60 - 10, System.currentTimeMillis() - 1000 * 60, new HostPort("127.0.0.1", 443), new HostPort("192.168.0.2", 443)),
                            new PingStatsResult(System.currentTimeMillis() - 1000 * 60 - 10, System.currentTimeMillis() - 1000 * 60, new HostPort("127.0.0.1", 443), new HostPort("192.168.0.3", 443))));

            return Lists.newArrayList(collector1, collector2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }

    @Override
    public void remove(ProxyModel proxyModel) {

    }

    private XpipeNettyClientKeyedObjectPool objectPool;
    protected XpipeNettyClientKeyedObjectPool getXpipeNettyClientKeyedObjectPool() throws Exception {


        if(objectPool == null) {
            objectPool = new XpipeNettyClientKeyedObjectPool();
            LifecycleHelper.initializeIfPossible(objectPool);
            LifecycleHelper.startIfPossible(objectPool);
        }

        return objectPool;
    }

    @Override
    public void isCrossDcLeader() {

    }

    @Override
    public void notCrossDcLeader() {

    }
}

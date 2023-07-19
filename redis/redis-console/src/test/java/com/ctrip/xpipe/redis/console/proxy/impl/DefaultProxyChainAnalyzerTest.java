package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultProxyChainAnalyzerTest extends AbstractProxyChainTest {

    private DefaultProxyChainAnalyzer analyzer = new DefaultProxyChainAnalyzer();

    private ProxyMonitorCollectorManager manager = mock(ProxyMonitorCollectorManager.class);

    private MetaCache metaCache = mock(MetaCache.class);

    private RouteService routeService = mock(RouteService.class);

    @Before
    public void beforeDefaultProxyChainAnalyzerTest() {

        ProxyMonitorCollector collector1 = mock(ProxyMonitorCollector.class);
        ProxyMonitorCollector collector2 = mock(ProxyMonitorCollector.class);
        String tunnelId1 = generateTunnelId();
        String tunnelId2 = generateTunnelId();
        when(collector1.getTunnelInfos()).thenReturn(Lists.newArrayList(new DefaultTunnelInfo(getProxy("SHAOY"), tunnelId1)
                .setTunnelSocketStatsResult(genTunnelSSR(tunnelId1)).setTunnelStatsResult(genTunnelSR(tunnelId1))));
        when(collector2.getTunnelInfos()).thenReturn(Lists.newArrayList(new DefaultTunnelInfo(getProxy("FRA-AWS"), tunnelId2)
                .setTunnelSocketStatsResult(genTunnelSSR(tunnelId2)).setTunnelStatsResult(genTunnelSR(tunnelId2))));
        when(manager.getProxyMonitorResults()).thenReturn(Lists.newArrayList(collector1, collector2));

        String cluster = "cluster", shard = "shard";
        when(metaCache.findClusterShard(any(HostPort.class))).thenReturn(new Pair<>(cluster, shard));
        when(metaCache.getDc(any(HostPort.class))).thenReturn("SHAOY");
        when(metaCache.isMetaChain(any(HostPort.class), any(HostPort.class))).thenReturn(true);

        when(routeService.existsRouteBetweenDc(anyString(), anyString())).thenReturn(false);
        when(routeService.existsRouteBetweenDc("SHAOY", "FRA-AWS")).thenReturn(true);

        analyzer.setRouteService(routeService);
        analyzer.setExecutors(executors);
        analyzer.setMetaCache(metaCache);
        analyzer.setProxyMonitorCollectorManager(manager);

        analyzer.fullUpdate();
        sleep(100);
    }

    @Test
    public void testGetClusterShardChainMap() {
        Map<DcClusterShardPeer, ProxyChain> clusterShardChainMap = analyzer.getClusterShardChainMap();
        Assert.assertNotNull(clusterShardChainMap);
        logger.info("[chain] {}", clusterShardChainMap);
    }
}
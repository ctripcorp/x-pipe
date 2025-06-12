package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultHealthCheckEndpointFactoryTest extends AbstractRedisTest {

    private DefaultHealthCheckEndpointFactory factory;

    @Mock
    private MetaCache metaCache;

    @Mock
    ProxyChecker proxyChecker;

    @Mock
    CheckerConfig config;

    RouteChooseStrategyFactory routeChooseStrategyFactory = new DefaultRouteChooseStrategyFactory();

    @Before
    public void beforeDefaultHealthCheckEndpointFactoryTest() {
        this.factory = new DefaultHealthCheckEndpointFactory(proxyChecker, config, metaCache, routeChooseStrategyFactory);
    }

    @Test
    public void testGetOrCreateEndpoint() {
        when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(null);
        HostPort hostport = localHostport(randomPort());
        when(metaCache.getDc(hostport)).thenReturn("oy");
        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertFalse(endpoint instanceof ProxyEnabled);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());
        logger.info("[endpoint] {}", endpoint);
        factory.remove(hostport);
    }

    @Test
    public void testGetOrCreateEndpointWithProxyProtocol() {
        String routeInfo1 = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8998";
        String routeInfo2 = "PROXYTCP://127.0.0.2:8008,PROXYTCP://127.0.0.2:8998";
        RouteMeta routeMeta1 = new RouteMeta().setRouteInfo(routeInfo1).setDstDc("oy").setIsPublic(true).setClusterType("").setOrgId(0);
        RouteMeta routeMeta2 = new RouteMeta().setRouteInfo(routeInfo2).setDstDc("oy").setIsPublic(false).setClusterType("").setOrgId(0);
        DcMeta dcMeta = new DcMeta("oy");
        ClusterMeta clusterMeta = new ClusterMeta("cluster1").setType("one_way").setOrgId(1);
        dcMeta.addCluster(clusterMeta);
        XpipeMetaManager.MetaDesc metaDesc = Mockito.mock(XpipeMetaManager.MetaDesc.class);
        when(metaDesc.getClusterMeta()).thenReturn(clusterMeta);
        when(metaDesc.getDcId()).thenReturn("oy");
        when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(Lists.newArrayList(routeMeta1, routeMeta2));
        HostPort hostport = localHostport(randomPort());
        when(metaCache.getDc(hostport)).thenReturn("oy");
        when(metaCache.findMetaDesc(hostport)).thenReturn(metaDesc);
        factory.updateRoutes();
        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
//        Assert.assertTrue(endpoint instanceof ProxyEnabled);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());

        String[] expected = StringUtil.splitRemoveEmpty("\\s*,\\s*", routeInfo1);
        Arrays.sort(expected);
        ProxyResourceManager proxyResourceManager = ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort());
        List<ProxyInetSocketAddress> endpoints = proxyResourceManager.nextEndpoints();
        String[] actual = new String[endpoints.size()];
        int index = 0;
        for(ProxyInetSocketAddress endpoint1 : endpoints) {
            actual[index ++] = String.format("PROXYTCP://%s:%s" , endpoint1.getAddress().getHostAddress(), endpoint1.getPort());
        }
        Arrays.sort(actual);
        Assert.assertTrue(Arrays.deepEquals(expected, actual));

        Assert.assertEquals(new String(proxyResourceManager.getProxyConnectProtocol()),
                String.format("+PROXY ROUTE TCP://%s:%d;\r\n", hostport.getHost(), hostport.getPort()));

        //route switch from route1 --->route2
        routeMeta1.setIsPublic(false);
        routeMeta2.setIsPublic(true);
        factory.updateRoutes();
        endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());
        expected = StringUtil.splitRemoveEmpty("\\s*,\\s*", routeInfo2);
        Arrays.sort(expected);
        proxyResourceManager = ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort());
        endpoints = proxyResourceManager.nextEndpoints();
        actual = new String[endpoints.size()];
        index = 0;
        for(ProxyInetSocketAddress endpoint1 : endpoints) {
            actual[index ++] = String.format("PROXYTCP://%s:%s" , endpoint1.getAddress().getHostAddress(), endpoint1.getPort());
        }
        Arrays.sort(actual);
        Assert.assertTrue(Arrays.deepEquals(expected, actual));

        Assert.assertEquals(new String(proxyResourceManager.getProxyConnectProtocol()),
                String.format("+PROXY ROUTE TCP://%s:%d;\r\n", hostport.getHost(), hostport.getPort()));

        //route switch from route2 --->route1
        routeMeta1.setIsPublic(true);
        routeMeta2.setIsPublic(false);
        factory.updateRoutes();
        endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());
        expected = StringUtil.splitRemoveEmpty("\\s*,\\s*", routeInfo1);
        Arrays.sort(expected);
        proxyResourceManager = ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort());
        endpoints = proxyResourceManager.nextEndpoints();
        actual = new String[endpoints.size()];
        index = 0;
        for(ProxyInetSocketAddress endpoint1 : endpoints) {
            actual[index ++] = String.format("PROXYTCP://%s:%s" , endpoint1.getAddress().getHostAddress(), endpoint1.getPort());
        }
        Arrays.sort(actual);
        Assert.assertTrue(Arrays.deepEquals(expected, actual));

        Assert.assertEquals(new String(proxyResourceManager.getProxyConnectProtocol()),
                String.format("+PROXY ROUTE TCP://%s:%d;\r\n", hostport.getHost(), hostport.getPort()));

        logger.info("[endpoint] {}", endpoint);
        logger.info("[protocol] {}", new String(proxyResourceManager.getProxyConnectProtocol()));
        factory.remove(hostport);
        Assert.assertTrue(ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort()) == null);
    }

    @Test
    public void testCreateInstanceBeforeChooserInited() {
        String routeInfo1 = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8998";
        RouteMeta routeMeta1 = new RouteMeta().setRouteInfo(routeInfo1).setDstDc("oy").setIsPublic(true).setClusterType("").setOrgId(0);
        DcMeta dcMeta = new DcMeta("oy");
        ClusterMeta clusterMeta = new ClusterMeta("cluster1").setType("one_way").setOrgId(1);
        dcMeta.addCluster(clusterMeta);
        XpipeMetaManager.MetaDesc metaDesc = Mockito.mock(XpipeMetaManager.MetaDesc.class);
        when(metaDesc.getClusterMeta()).thenReturn(clusterMeta);
        when(metaDesc.getDcId()).thenReturn("oy");
        when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(Lists.newArrayList(routeMeta1));
        HostPort hostport = localHostport(randomPort());
        when(metaCache.findMetaDesc(hostport)).thenReturn(metaDesc);

        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());

        ProxyResourceManager proxyResourceManager = ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort());
        Assert.assertNotNull(proxyResourceManager);

    }


}
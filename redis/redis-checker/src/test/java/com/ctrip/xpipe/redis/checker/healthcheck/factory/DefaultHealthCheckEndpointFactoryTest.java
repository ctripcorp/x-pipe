package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthCheckEndpointFactoryTest extends AbstractRedisTest {

    @InjectMocks
    private DefaultHealthCheckEndpointFactory factory = new DefaultHealthCheckEndpointFactory();

    @Mock
    private MetaCache metaCache;

    @Before
    public void beforeDefaultHealthCheckEndpointFactoryTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOrCreateEndpoint() {
        when(metaCache.getRoutes()).thenReturn(null);
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
    public void testGetOrCreateProxyEnabledEndpoint() {
        String routeInfo = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8998";
        when(metaCache.getRoutes()).thenReturn(Lists.newArrayList(new RouteMeta().setRouteInfo(routeInfo).setDstDc("oy")));
        HostPort hostport = localHostport(randomPort());
        when(metaCache.getDc(hostport)).thenReturn("oy");
        factory.updateRoutes();
        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
//        Assert.assertTrue(endpoint instanceof ProxyEnabled);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());

        String[] expected = StringUtil.splitRemoveEmpty("\\s*,\\s*", routeInfo);
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

        logger.info("[endpoint] {}", endpoint);
        logger.info("[protocol] {}", new String(proxyResourceManager.getProxyConnectProtocol()));
        factory.remove(hostport);
        Assert.assertTrue(ProxyRegistry.getProxy(hostport.getHost(), hostport.getPort()) == null);
    }
    
    @Test
    public void testChecker() {
        
    }
}
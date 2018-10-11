package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
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
        when(metaCache.getRouteIfPossible(any())).thenReturn(null);
        HostPort hostport = localHostport(randomPort());
        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertFalse(endpoint instanceof ProxyEnabled);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());
        logger.info("[endpoint] {}", endpoint);
    }

    @Test
    public void testGetOrCreateProxyEnabledEndpoint() {
        String routeInfo = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8998";
        when(metaCache.getRouteIfPossible(any())).thenReturn(new RouteMeta().setRouteInfo(routeInfo));
        HostPort hostport = localHostport(randomPort());
        Endpoint endpoint = factory.getOrCreateEndpoint(hostport);
        Assert.assertTrue(endpoint instanceof ProxyEnabled);
        Assert.assertEquals(hostport.getHost(), endpoint.getHost());
        Assert.assertEquals(hostport.getPort(), endpoint.getPort());

        String[] expected = StringUtil.splitRemoveEmpty("\\s*,\\s*", routeInfo);
        Arrays.sort(expected);

        List<ProxyEndpoint> endpoints = ((ProxyEnabled) endpoint).getProxyProtocol().nextEndpoints();
        String[] actual = new String[endpoints.size()];
        int index = 0;
        for(ProxyEndpoint endpoint1 : endpoints) {
            actual[index ++] = endpoint1.getUri();
        }
        Arrays.sort(actual);
        Assert.assertTrue(Arrays.deepEquals(expected, actual));

        Assert.assertEquals(((ProxyEnabled)endpoint).getProxyProtocol().getRouteInfo(),
                String.format("%s TCP://%s:%d", routeInfo, hostport.getHost(), hostport.getPort()));

        logger.info("[endpoint] {}", endpoint);
        logger.info("[protocol] {}", ((ProxyEnabled) endpoint).getProxyProtocol());
    }
}
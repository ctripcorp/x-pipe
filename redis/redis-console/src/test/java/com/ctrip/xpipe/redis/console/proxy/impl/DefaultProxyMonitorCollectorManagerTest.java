package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.junit.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultProxyMonitorCollectorManagerTest extends AbstractRedisTest {

    private DefaultProxyMonitorCollectorManager manager = new DefaultProxyMonitorCollectorManager();

    private RouteService routeService = mock(RouteService.class);

    private ProxyService proxyService = mock(ProxyService.class);

    private int index = 0;

    @Before
    public void beforeDefaultProxyMonitorCollectorManagerTest() throws Exception {
        manager.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool());
        manager.setScheduled(scheduled);
        manager.setProxyService(proxyService);
        manager.setRouteService(routeService);
    }

    @After
    public void afterDefaultProxyMonitorCollectorManagerTest() {
        index = 0;
    }

    @Test
    public void testGetOrCreate() {
        ProxyMonitorCollector collector = manager.getOrCreate(
                new ProxyModel().setActive(true).setDcName("dc").setId(1L).setUri("PROXYTCP://10.26.188.174:80"));
        Assert.assertNotNull(collector);

        manager.getOrCreate(new ProxyModel().setActive(true).setDcName("dc").setId(1L).setUri("PROXYTCP://10.26.188.174:80"));
        Assert.assertEquals(1, manager.getProxyMonitorResults().size());
    }

    @Test
    public void testUpdateWithTLSNotSelect() {
        when(proxyService.getActiveProxies()).thenReturn(Lists.newArrayList(
                newProxyModel("PROXYTLS://127.0.0.1:443"),
                newProxyModel("PROXYTLS://127.0.0.2:443"),
                newProxyModel("PROXYTLS://127.0.0.3:443")));
        when(routeService.getActiveRoutes()).thenReturn(newRouteModel());
        manager.update();
        Assert.assertTrue(manager.getProxyMonitorResults().isEmpty());
    }

    @Test
    public void testUpdateWithTCPSelect() {
        when(proxyService.getActiveProxies()).thenReturn(Lists.newArrayList(
                newProxyModel("PROXYTCP://127.0.0.1:443"),
                newProxyModel("PROXYTCP://127.0.0.2:443"),
                newProxyModel("PROXYTCP://127.0.0.3:443")));
        when(routeService.getActiveRoutes()).thenReturn(newRouteModel());
        manager.update();
        Assert.assertFalse(manager.getProxyMonitorResults().isEmpty());
        Assert.assertEquals(3, manager.getProxyMonitorResults().size());
    }

    @Test
    public void testUpdateWithNoProxyInterest() {
        when(proxyService.getActiveProxies()).thenReturn(Lists.newArrayList(
                newProxyModel("PROXYTCP://127.0.0.1:443"),
                newProxyModel("PROXYTCP://127.0.0.2:443"),
                newProxyModel("PROXYTCP://127.0.0.3:443")));
        when(routeService.getActiveRoutes()).thenReturn(Lists.newArrayList());
        manager.update();
        Assert.assertTrue(manager.getProxyMonitorResults().isEmpty());
    }

    private ProxyModel newProxyModel(String uri) {
        return new ProxyModel().setActive(true).setDcName("dc").setId(index ++).setUri(uri);
    }

    private List<RouteModel> newRouteModel() {
        int offset = index >> 1;
        return Lists.newArrayList(new RouteModel().setSrcProxyIds(ids(0, offset)).setTag(Route.TAG_META),
                new RouteModel().setSrcProxyIds(ids(offset + 1, index)).setTag(Route.TAG_META));
    }

    private String ids(int start, int end) {
        if(start > end) {
            return "";
        }
        String[] strs = new String[end - start + 1];
        int idx = 0;
        for(int i = start; i <= end; i++) {
            strs[idx++] = i + "";
        }
        return StringUtil.join(",", strs);
    }
}
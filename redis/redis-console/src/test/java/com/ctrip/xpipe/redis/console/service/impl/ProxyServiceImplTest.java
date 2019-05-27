package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
public class ProxyServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ProxyServiceImpl service;

    private ProxyModel proxy1, proxy2;

    @Before
    public void beforeProxyServiceImplTest() {
        proxy1 = new ProxyModel().setActive(true).setDcName(dcNames[0]).setId(1).setUri("PROXYTCP://127.0.0.1:8080");
        proxy2 = new ProxyModel().setActive(false).setDcName(dcNames[0]).setId(2).setUri("PROXYTCP://127.0.0.1:8080");

        service.addProxy(proxy1);
        service.addProxy(proxy2);
    }

    @Test
    public void testGetActiveProxies() {
        List<ProxyModel> routes = service.getAllProxies();
        Collections.sort(routes, new Comparator<ProxyModel>() {
            @Override
            public int compare(ProxyModel o1, ProxyModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(proxy1, proxy2), routes);
    }

    @Test
    public void testGetAllProxies() {
        List<ProxyModel> routes = service.getActiveProxies();
        Collections.sort(routes, new Comparator<ProxyModel>() {
            @Override
            public int compare(ProxyModel o1, ProxyModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(proxy1), routes);
    }

    @Test
    public void testUpdateProxy() {
        String newUri = "TCP://127.0.0.1:6379";
        proxy1.setUri(newUri);
        service.updateProxy(proxy1);

        ProxyModel proxy = null;
        for(ProxyModel mode : service.getAllProxies()) {
            if(mode.getId() == proxy1.getId()) {
                proxy = mode;
                break;
            }
        }
        Assert.assertEquals(newUri, proxy.getUri());
    }

    @Test
    public void testDeleteProxy() {
        service.deleteProxy(proxy1.getId());
        Assert.assertEquals(Lists.newArrayList(proxy2), service.getAllProxies());
    }

    @Test
    public void testAddProxy() {
        ProxyModel proxy3 = new ProxyModel().setActive(false).setDcName(dcNames[0]).setId(3).setUri("PROXYTCP://127.0.0.1:8080");
        service.addProxy(proxy3);
        Assert.assertEquals(Lists.newArrayList(proxy1, proxy2, proxy3), service.getAllProxies());
    }

    @Test
    public void testGetActiveProxyTbls() {
        List<ProxyTbl> proxyTbls = service.getActiveProxyTbls();
        for(ProxyTbl proto : proxyTbls) {
            Assert.assertTrue(proto.isActive());
        }
    }

    @Test
    public void testGetProxyAll() {
        Assert.assertNotNull(service.getAllProxies());
    }
}
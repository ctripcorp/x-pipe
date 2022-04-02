package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
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
 * Jun 19, 2018
 */
public class ProxyDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ProxyDao proxyDao;

    private ProxyTbl proto1, proto2;

    @Before
    public void beforeProxyDaoTest() {
        Assert.assertEquals(0, proxyDao.getActiveProxyTbls().size());

        proto1 = new ProxyTbl().setActive(true).setDcId(1L).setUri("Proxy://127.0.0.1:8080");
        proto2 = new ProxyTbl().setActive(false).setDcId(1L).setUri("Proxy://127.0.0.1:8080");

        proxyDao.insert(proto1);
        proxyDao.insert(proto2);
    }
    @Test
    public void testGetAllProxyTbls() {

        List<ProxyTbl> proxies = proxyDao.getActiveProxyTbls();
        Assert.assertEquals(1, proxies.size());
        ProxyTbl target = proxies.get(0);
        Assert.assertEquals(proto1.getDcId(), target.getDcId());
        Assert.assertEquals(proto1.getUri(), target.getUri());
    }

    @Test
    public void testGetAllProxies() {
        List<ProxyTbl> proxies = proxyDao.getAllProxyTbls();
        Collections.sort(proxies, new Comparator<ProxyTbl>() {
            @Override
            public int compare(ProxyTbl o1, ProxyTbl o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(2, proxies.size());
    }

    @Test
    public void testUpdateProxy() {
        String newUri = "TCP://127.0.0.1:6379";
        proto1.setUri(newUri);
        proxyDao.update(proto1);

        ProxyTbl proto = null;
        for(ProxyTbl proxy : proxyDao.getAllProxyTbls()) {
            if(proxy.getId() == proto1.getId()) {
                proto = proxy;
                break;
            }
        }
        Assert.assertNotNull(proto);
        Assert.assertEquals(newUri, proto.getUri());
    }

    @Test
    public void testGetActiveProxyTblsByDc() {
        List<ProxyTbl> proxies = proxyDao.getActiveProxyTblsByDc(1L);
        Assert.assertEquals(1, proxies.size());

        proxies = proxyDao.getActiveProxyTblsByDc(2L);
        Assert.assertEquals(0, proxies.size());
    }
}
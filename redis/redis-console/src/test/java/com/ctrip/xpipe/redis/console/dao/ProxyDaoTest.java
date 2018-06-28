package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.core.entity.Route;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public class ProxyDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ProxyDao proxyDao;

    @Test
    public void testGetAllProxyTbls() {
        Assert.assertEquals(0, proxyDao.getAllProxyTbls().size());

        ProxyTbl proto1 = new ProxyTbl().setActive(true).setDcId(1L).setUri("Proxy://127.0.0.1:8080");
        ProxyTbl proto2 = new ProxyTbl().setActive(false).setDcId(1L).setUri("Proxy://127.0.0.1:8080");

        proxyDao.insert(proto1);
        proxyDao.insert(proto2);

        List<ProxyTbl> proxies = proxyDao.getAllProxyTbls();
        Assert.assertEquals(1, proxies.size());
        ProxyTbl target = proxies.get(0);
        Assert.assertEquals(proto1.getDcId(), target.getDcId());
        Assert.assertEquals(proto1.getUri(), target.getUri());
    }
}
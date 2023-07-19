package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;


public class ProxyServiceImplTest2 extends AbstractServiceImplTest {
    @Autowired
    private ProxyServiceImpl service;

    @Test
    public void testGetMonitorActiveProxiesByDc() {
        List<ProxyModel> proxies = service.getMonitorActiveProxiesByDc("jq");
        Assert.assertEquals(2, proxies.size());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/proxy-test.sql");
    }
}

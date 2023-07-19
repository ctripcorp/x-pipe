package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DefaultProxyChainCollectorTest extends AbstractConsoleTest {

    @Autowired
    ProxyChainCollector collector;

    @Test
    public void testUpdateProxyChains() {
//        collector.updateShardProxyChainMap();
    }

}
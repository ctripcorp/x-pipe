package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MockMetricProxy;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 14, 2017
 */
public class DefaultMetaServerConsoleServiceTest extends AbstractRedisTest{

    private MockMetricProxy metricProxy;

    @Before
    public void setupDefaultMetaServerConsoleServiceTest() {
        this.metricProxy = new MockMetricProxy();
    }

    @Test
    public void test(){

        try {
            logger.info("[begin]");
            MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(new MetaserverAddress("oy", "http://10.0.0.1:1234"));
            consoleService.doChangePrimaryDc("cluster1", "shard-0", "oy", null);
        }catch (Exception e){
            logger.error("[Exception]", e);
        }

    }

    @Test
    public void testTimeout(){

        try {
            logger.info("[begin]");
            DefaultMetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(new MetaserverAddress("oy", "http://10.0.0.1:1234"));
            consoleService.setMetricProxy(metricProxy);
            consoleService.doChangePrimaryDc("cluster1", "shard-0", "oy", null);
        }catch (Exception e){
            logger.error("[Exception]", e);
        }

        MetricData metricData = metricProxy.poll();
        Assert.assertEquals("oy", metricData.getDcName());
        Assert.assertEquals("cluster1", metricData.getClusterName());
        Assert.assertEquals("changePrimaryDc", metricData.getTags().get("api"));
        Assert.assertEquals("FAIL", metricData.getTags().get("status"));
    }
}

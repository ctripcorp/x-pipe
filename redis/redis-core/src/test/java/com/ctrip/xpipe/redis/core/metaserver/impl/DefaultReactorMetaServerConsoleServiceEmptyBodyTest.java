package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MockMetricProxy;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DefaultReactorMetaServerConsoleServiceEmptyBodyTest extends AbstractTest {

    private DefaultReactorMetaServerConsoleService service;

    private MockWebServer webServer;

    private MockMetricProxy metricProxy;

    private String cluster = "cluster1", shard = "shard1", targetDc = "oy";

    @Before
    public void setUp() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        String metaserverAddress = "http://127.0.0.1:" + webServer.getPort();
        service = new DefaultReactorMetaServerConsoleService(
                new MetaserverAddress(targetDc, metaserverAddress), loopResources, connectionProvider);
        metricProxy = new MockMetricProxy();
        service.setMetricProxy(metricProxy);
    }

    @Test
    public void makeMasterReadOnlyEmptyBodyShouldFailFast() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("")
                .setHeader("Content-Type", "application/json"));

        try {
            service.makeMasterReadOnly(cluster, shard, true).get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            Assert.assertTrue(e.getCause().getMessage().contains("empty response"));
        }

        MetricData metricData = metricProxy.poll();
        Assert.assertEquals("makeMasterReadOnly", metricData.getTags().get("api"));
        Assert.assertEquals("FAIL", metricData.getTags().get("status"));
    }

    @Test
    public void changePrimaryDcCheckEmptyBodyShouldFailFast() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("")
                .setHeader("Content-Type", "application/json"));

        try {
            service.changePrimaryDcCheck(cluster, shard, targetDc).get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            Assert.assertTrue(e.getCause().getMessage().contains("empty response"));
        }

        MetricData metricData = metricProxy.poll();
        Assert.assertEquals("changePrimaryDcCheck", metricData.getTags().get("api"));
        Assert.assertEquals("FAIL", metricData.getTags().get("status"));
    }

    @Test
    public void doChangePrimaryDcEmptyBodyShouldFailFast() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("")
                .setHeader("Content-Type", "application/json"));

        try {
            service.doChangePrimaryDc(cluster, shard, targetDc, null).get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            Assert.assertTrue(e.getCause().getMessage().contains("empty response"));
        }

        MetricData metricData = metricProxy.poll();
        Assert.assertEquals("changePrimaryDc", metricData.getTags().get("api"));
        Assert.assertEquals("FAIL", metricData.getTags().get("status"));
    }
}

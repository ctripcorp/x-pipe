package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxyException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class HickwallMetricManualTest extends AbstractServiceTest{

    private HickwallMetric hickwallMetricProxy;

    @Before
    public void beforeHickwallMetricProxyTest(){
        hickwallMetricProxy = new HickwallMetric();
    }

    @Test
    public void testHickWall() throws MetricProxyException, IOException {

        int port = 11111;

        logger.info("[testHickWall]{}", port);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                HostPort hostPort = new HostPort("127.0.0.1", port);
                MetricData metricData = new MetricData("retrans", "dc", "cluster", "shard");
                metricData.setValue(1000);
                metricData.setHostPort(hostPort);
                metricData.setTimestampMilli(System.currentTimeMillis());

                hickwallMetricProxy.writeBinMultiDataPoint(metricData);
            }
        }, 0, 2, TimeUnit.SECONDS);


        waitForAnyKeyToExit();
    }

    @Test
    public void testGetFormattedRedisAddr() {
        HostPort hostPort = new HostPort("10.2.2.2", 6379);
        String result = hickwallMetricProxy.getFormattedRedisAddr(hostPort);
        Assert.assertEquals("10_2_2_2_6379", result);
    }

    @Test
    public void testGetFormattedSrcAddr() {
        String ip = "192.168.0.1";
        Assert.assertEquals("192_168_0_1", hickwallMetricProxy.getFormattedSrcAddr(ip));
    }

}

package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuchen
 * Apr 2020/4/9 2020
 */
public class NewHickwallMetricTest extends AbstractServiceTest {

    private NewHickwallMetric hickwallMetricProxy;

    @Before
    public void beforeHickwallMetricProxyTest(){
        hickwallMetricProxy = new NewHickwallMetric();
    }

    @Test
    public void testHickWall() throws MetricProxyException, IOException {

        int port = 11111;

        logger.info("[testHickWall]{}", port);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                HostPort hostPort = new HostPort("127.0.0.1", port);
                MetricData metricData = new MetricData("fx.xpipe.delay", "dc", "cluster", "shard");
                metricData.setValue(1000);
                metricData.setHostPort(hostPort);
                metricData.setTimestampMilli(System.currentTimeMillis());

                hickwallMetricProxy.writeBinMultiDataPoint(metricData);
            }
        }, 0, 2, TimeUnit.SECONDS);


        waitForAnyKeyToExit();
    }
}

package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxyException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class HickwallMetricTest extends AbstractServiceTest{

    private HickwallMetric hickwallMetricProxy;

    @Before
    public void beforeHickwallMetricProxyTest(){
        hickwallMetricProxy = new HickwallMetric();
    }

    @Test
    public void testHickWall() throws MetricProxyException, IOException {

        int port = randomPort();

        logger.info("[testHickWall]{}", port);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                List<MetricData> data = new LinkedList<>();
                MetricData metricData = new MetricData("test", getTestName());
                metricData.setValue(1000);
                metricData.setHostPort(new HostPort("127.0.0.1", port));
                metricData.setTimestampMilli(System.currentTimeMillis());
                data.add(metricData);
                hickwallMetricProxy.writeBinMultiDataPoint(data);
            }
        }, 0, 2, TimeUnit.SECONDS);


        waitForAnyKeyToExit();
    }

}

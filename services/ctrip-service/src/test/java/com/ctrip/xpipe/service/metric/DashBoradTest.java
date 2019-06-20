package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.clogging.agent.MessageManager;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregator;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregatorFactory;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Test;

import java.util.Random;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class DashBoradTest extends AbstractServiceTest{

    private static MetricsAggregator aggregator2 = MetricsAggregatorFactory.createAggregator("dashboard.demo.xpipe2", "x", "y", "z");

    @Test
    public void testDashboard() throws InterruptedException {

        Random rnd = new Random();

        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            for (int j = 0; j < 100; j++) {
                for (int k = 0; k < 10; k++) {
                    //add(sum,count,tags)
                    aggregator2.add(rnd.nextInt(100), 10, "x" + k, "y" + k, "z" + k);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Thread.sleep(1000000);
        MessageManager.getInstance().shutdown();
    }
}

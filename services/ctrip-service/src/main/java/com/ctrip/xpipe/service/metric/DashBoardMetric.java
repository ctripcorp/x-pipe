package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.clogging.agent.MessageManager;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregator;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregatorFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class DashBoardMetric implements MetricProxy{

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static MetricsAggregator aggregator = MetricsAggregatorFactory.createAggregator("fx.xpipe.delay", "console-dc", "dc", "cluster", "shard", "ip", "port");

    public DashBoardMetric(){

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("[run][shutdown]");
                MessageManager.getInstance().shutdown();
            }
        }));
    }


    @Override
    public void writeBinMultiDataPoint(MetricData metricData) throws MetricProxyException {

        aggregator.add((long)metricData.getValue(),
                FoundationService.DEFAULT.getDataCenter(),
                metricData.getDcName(),
                metricData.getClusterName(),
                metricData.getShardName(),
                metricData.getHostPort().getHost(),
                String.valueOf(metricData.getHostPort().getPort()));

    }

    @Override
    public int getOrder() {
        return 0;
    }
}

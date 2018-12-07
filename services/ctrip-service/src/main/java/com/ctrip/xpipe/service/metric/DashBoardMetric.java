package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.clogging.agent.MessageManager;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregator;
import com.ctrip.framework.clogging.agent.metrics.aggregator.MetricsAggregatorFactory;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class DashBoardMetric implements MetricProxy{

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, MetricsAggregator> aggregators = Maps.newConcurrentMap();

    public DashBoardMetric(){

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("[run][shutdown]");
                MessageManager.getInstance().shutdown();
            }
        }));
        aggregators.put("delay", createAggregator("delay"));
    }


    @Override
    public void writeBinMultiDataPoint(MetricData metricData) throws MetricProxyException {

        MetricsAggregator aggregator = MapUtils.getOrCreate(aggregators, metricData.getMetricType(),
                new ObjectFactory<MetricsAggregator>() {
            @Override
            public MetricsAggregator create() {
                return createAggregator(metricData.getMetricType());
            }
        });
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

    private MetricsAggregator createAggregator(String metricType) {
        String mesurement = String.format("fx.xpipe.%s", metricType);
        return MetricsAggregatorFactory.createAggregator(mesurement, "console-dc", "dc", "cluster", "shard", "ip", "port");
    }
}

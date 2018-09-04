package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
@Component
public class MetricDelayCollector implements DelayCollector {

    private static final Logger logger = LoggerFactory.getLogger(MetricDelayCollector.class);

    private static final String TYPE = "delay";

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    @Override
    public void collect(RedisHealthCheckInstance instance) {
        try {
            proxy.writeBinMultiDataPoint(Lists.newArrayList(getPoint(instance)));
        } catch (Exception e) {
            logger.error("Error send metrics to metric", e);
        }
    }

    private MetricData getPoint(RedisHealthCheckInstance instance) {

        MetricData data = new MetricData(TYPE, instance.getRedisInstanceInfo().getClusterId(),
                instance.getRedisInstanceInfo().getShardId());
        DelayContext context = instance.getHealthCheckContext().getDelayContext();
        data.setValue(context.lastDelayNano()/1000);
        data.setTimestampMilli(System.currentTimeMillis());
        data.setHostPort(instance.getRedisInstanceInfo().getHostPort());
        return data;
    }
}
